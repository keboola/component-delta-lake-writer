import logging
import os
import time
import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableOperation
import databricks.sdk.errors as dbx_errors


import duckdb
from deltalake import write_deltalake, WriterProperties
from duckdb.duckdb import DuckDBPyConnection
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from configuration import Configuration

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self._connection = self.init_connection()
        self._uc_client = None
        self.table = None

    def run(self):
        tables = self.get_input_tables_definitions()
        files = self.get_input_files_definitions()

        if (tables and files) or (not tables and not files):
            raise Exception("Each configuration row can be mapped to either a file or a table, but not both.")

        if len(tables) > 1:
            raise UserException("Each configuration row can have only one input table")

        relation = None
        if tables:
            self.table = tables[0]
            if self.table.is_sliced:
                path = f"{self.table.full_path}/*.csv"
            else:
                path = self.table.full_path

            dtypes = {key: value.data_types.get("base").dtype for key, value in self.table.schema.items()}
            relation = self._connection.read_csv(path, dtype=dtypes)

        if files:
            files_paths = [file.full_path for file in files]
            relation = self._connection.read_parquet(files_paths)

        batches = relation.select("order_id").fetch_arrow_reader(batch_size=self.params.batch_size)

        storage_options, uri = self._get_storage_options_and_uri()

        writer_properties = WriterProperties(
            data_page_size_limit=8 * 1024 * 1024,
            compression=self.params.destination.compression,
        )

        line = self.params.batch_size
        logging.info(f"Writing records {line} - {line + self.params.batch_size}")
        self.write_batch(
            uri,
            next(batches),
            self.params.destination.mode.value,
            storage_options,
            writer_properties,
            self.params.destination.partition_by,
            schema_mode="overwrite",
        )
        line += self.params.batch_size

        for batch in batches:
            logging.info(f"Writing records {line} - {line + self.params.batch_size}")
            self.write_batch(
                uri,
                batch,
                "append",
                storage_options,
                writer_properties,
                self.params.destination.partition_by,
            )
            line += self.params.batch_size

        self._connection.close()

        # update Unity Catalog metadata from the delta table
        dest = self.params.destination
        self._execute_query(
            dest=dest, query=f"MSCK REPAIR TABLE {dest.catalog}.{dest.schema_name}.{dest.table} SYNC METADATA;"
        )

    def _get_temp_credentials(self):
        dest = self.params.destination
        table_full_name = f"{dest.catalog}.{dest.schema_name}.{dest.table}"

        if not self._uc_client.tables.exists(full_name=table_full_name).table_exists:
            self._create_table(dest)

        table_details = self._uc_client.tables.get(full_name=table_full_name)
        table_id = table_details.table_id
        region = self._uc_client.metastores.get(table_details.metastore_id).region

        try:
            creds = self._uc_client.temporary_table_credentials.generate_temporary_table_credentials(
                operation=TableOperation.READ_WRITE, table_id=table_id
            )
            return creds, region
        except dbx_errors.platform.PermissionDenied as e:
            raise UserException(f"Permission denied: {str(e)}")

    def _build_stage_query(self):
        column_defs = []
        for idx, (col_name, col_def) in enumerate(self.table.schema.items()):
            dtype = col_def.data_types["base"].dtype
            column_defs.append(f"_c{idx} {dtype}")

        columns_str = ", ".join(column_defs)
        stage_query = f"""
    CREATE OR REPLACE TABLE staging_table ({columns_str});
    """
        return stage_query

    def _build_copy_query(self, dest):
        select_lines = []
        for idx, col_name in enumerate(self.table.schema.keys()):
            select_lines.append(f"  col_{idx}   AS {col_name}")

        select_clause = ",\n".join(select_lines)

        # partition_columns = dest.partition_by.join(", ") if dest.partition_by else ""

        stage_query = f"""
    CREATE TABLE {dest.catalog}.{dest.schema_name}.{dest.table}
    USING DELTA
    --PARTITIONED BY (partition_columns)
    AS
    SELECT
    {select_clause}
    FROM staging_table;
        """
        return stage_query

    def _create_table(self, dest):
        """
        # TODO This should have the best performance and will create native Delta table in the Unity Catalog.
        Requires component with S3 stagingem, similarly the insert and upsert need to be implemented with staging table.
        Then the component will always access the Unity Catalog, but when using CSV staging with external
        delta table we will use write_deltalake otherwise we execute SQL inside DBX to load data directly from S3.
        """

#         #can't write to the Unity Catalog from outside of the DBX
#         metastore_id = self._uc_client.schemas.get(full_name=f"{dest.catalog}.{dest.schema_name}").metastore_id
#         storage_root = self._uc_client.metastores.get(metastore_id)
#
#         query = f"""CREATE TABLE {dest.catalog}.{dest.schema_name}.{dest.table} (is_active STRING, id string)
#                     LOCATION '{storage_root}/{dest.schema_name}/{dest.table}'
# --                     PARTITIONED BY (is_active)
#                     TBLPROPERTIES ('delta.enableDeletionVectors' = false);
#                     WITH (CREDENTIALS)
#
#                     """  # delta-rs doesn't support del. vectors
#
#         credential = self._uc_client.storage_credentials.create(
#             name=f"ext-tbl-{dest.catalog}_{dest.schema_name}_{dest.table}",
#         )
#
#         created = self._uc_client.external_locations.create(
#             name=f"sdk-{time.time_ns()}",
#             credential_name=credential.name,
#             url="s3://%s/%s" % (os.environ["TEST_BUCKET"], f"sdk-{time.time_ns()}"),
#         )

        s3 = json.load(open(f"{self.table.full_path}.manifest")).get("s3", {})
        aws_key = s3.get("credentials", {}).get("access_key_id")
        aws_secret = s3.get("credentials", {}).get("secret_access_key")
        aws_token = s3.get("credentials", {}).get("session_token")
        s3_bucket = s3.get("bucket")
        s3_key = s3.get("key")

        self._connection.execute(f"""
            CREATE OR REPLACE SECRET (
                TYPE S3,
                REGION '{s3.get("region")}',
                KEY_ID '{aws_key}',
                SECRET '{aws_secret}',
                SESSION_TOKEN '{aws_token}'
                );
                """)

        # get paths to csv files from the manifest
        manifest = self._connection.sql(f"FROM read_json('s3://{s3_bucket}/{s3_key}')").fetchone()[0]
        files = [f.get("url") for f in manifest]

        self._execute_query(dest, self._build_stage_query())

        load_query = f"""
        COPY INTO staging_table
        FROM '{files[0]}' WITH (
          CREDENTIAL (AWS_ACCESS_KEY = '{aws_key}',
                      AWS_SECRET_KEY = '{aws_secret}',
                      AWS_SESSION_TOKEN = '{aws_token}')
        )
        FILEFORMAT = CSV
        FORMAT_OPTIONS (
          'header' = 'false',
          'inferSchema' = 'false',
          'mergeSchema' = 'false'
        );
        """

        self._execute_query(dest, load_query)

        self._execute_query(dest, self._build_copy_query(dest))

    def _execute_query(self, dest, query):
        logging.info(f"Executing query: {query}")

        # warehouse_id = self._uc_client.warehouses.list()

        res = self._uc_client.statement_execution.execute_statement(
            warehouse_id=self._uc_client.warehouses.list()[0].id,  # TODO implement some logic to select the warehouse
            catalog=dest.catalog,
            schema=dest.schema_name,
            statement=query,
        )

        statement_id = res.statement_id
        while res.status.state.value in ["PENDING", "RUNNING"]:
            time.sleep(5)
            logging.debug("Waiting for creating table to complete...")
            res = self._uc_client.statement_execution.get_statement(statement_id)

        if res.status.state.value == "FAILED":
            raise UserException(f"Failed to create table {dest.table}: {res.status.error}")

    def _get_storage_options_and_uri(self):
        storage_options = {
            "timeout": "3600s",
            "max_retries": "2",
        }

        match self.params.provider:
            case "abs":
                uri = f"az://{self.params.destination.container_name}/{self.params.destination.blob_name}"
                storage_options |= {
                    "azure_storage_account_name": self.params.abs_account_name,
                    "azure_storage_sas_token": self.params.abs_sas_token,
                }

            case "s3":
                uri = f"s3://{self.params.destination.container_name}/{self.params.destination.blob_name}"
                storage_options |= {
                    "aws_region": self.params.aws_region,
                    "aws_access_key_id": self.params.aws_key_id,
                    "aws_secret_access_key": self.params.aws_key_secret,
                }

            case "gcs":
                uri = f"gs://{self.params.destination.container_name}/{self.params.destination.blob_name}"
                storage_options |= {"google_service_account_key": self.params.gcp_service_account_key}

            case _:
                if self.params.access_method == "unity_catalog":
                    self._uc_client = WorkspaceClient(
                        host=self.params.unity_catalog_url, token=self.params.unity_catalog_token
                    )

                    temp_creds, region = self._get_temp_credentials()
                    uri = temp_creds.url

                    if temp_creds.azure_user_delegation_sas:
                        storage_options |= {
                            "azure_storage_account_name": temp_creds.url.split("@")[1].split(".")[0],
                            "azure_storage_sas_token": temp_creds.azure_user_delegation_sas.sas_token,
                        }
                    elif temp_creds.aws_temp_credentials:
                        storage_options |= {
                            "aws_region": region,
                            "aws_access_key_id": temp_creds.aws_temp_credentials.access_key_id,
                            "aws_secret_access_key": temp_creds.aws_temp_credentials.secret_access_key,
                            "aws_session_token": temp_creds.aws_temp_credentials.session_token,
                        }
                else:
                    raise UserException(f"Unknown provider: {self.params.provider}")

        return storage_options, uri

    @staticmethod
    def write_batch(table_or_uri, data, mode, storage_options, writer_properties, partition_by=None, schema_mode=None):
        start = time.time()
        write_deltalake(
            table_or_uri=table_or_uri,
            data=data,
            mode=mode,
            storage_options=storage_options,
            writer_properties=writer_properties,
            partition_by=partition_by,
            schema_mode=schema_mode,
        )
        logging.info(f"Batch written in {time.time() - start:.2f}s")

    def init_connection(self) -> DuckDBPyConnection:
        """
        Returns connection to temporary DuckDB database
        """
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        # TODO: On GCP consider changin tmp to /opt/tmp
        config = dict(
            temp_directory=DUCK_DB_DIR,
            extension_directory=os.path.join(DUCK_DB_DIR, "extensions"),
            threads=self.params.threads,
            max_memory=f"{self.params.max_memory}MB",
        )
        conn = duckdb.connect(config=config)

        if not self.params.preserve_insertion_order:
            conn.execute("SET preserve_insertion_order = false;").fetchall()

        return conn

    @sync_action("list_uc_catalogs")
    def list_uc_catalogs(self):
        w = WorkspaceClient(host=self.params.unity_catalog_url, token=self.params.unity_catalog_token)
        catalogs = w.catalogs.list()
        return [SelectElement(c.name) for c in catalogs]

    @sync_action("list_uc_schemas")
    def list_uc_schemas(self):
        w = WorkspaceClient(host=self.params.unity_catalog_url, token=self.params.unity_catalog_token)
        schemas = w.schemas.list(self.params.destination.catalog)
        return [SelectElement(s.name) for s in schemas]

    @sync_action("list_uc_tables")
    def list_uc_tables(self):
        w = WorkspaceClient(host=self.params.unity_catalog_url, token=self.params.unity_catalog_token)
        tables = w.tables.list(self.params.destination.catalog, self.params.destination.schema_name)
        return [SelectElement(t.name) for t in tables]

    @sync_action("access_method_helper")
    def access_method_helper(self):
        """
        Method to return the access method to the config row, so we can render UI based on the selected method.
        """
        return {
            "type": "data",
            "data": {
                "destination": {
                    "helper_access_method": self.params.access_method,
                    "container_name": self.params.source.container_name,
                    "blob_name": self.params.source.blob_name,
                    "catalog": self.params.source.catalog,
                    "schema_name": self.params.source.schema_name,
                    "table": self.params.source.table,
                    "mode": self.params.destination.mode.value,
                    "partition_by": self.params.destination.partition_by,
                    "compression": self.params.destination.compression,
                },
                "preserve_insertion_order": self.params.preserve_insertion_order,
                "batch_size": self.params.batch_size,
                "debug": self.params.debug,
            },
        }


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
