import logging
import os
import time
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableOperation
import databricks.sdk.errors as dbx_errors


import duckdb
from deltalake import write_deltalake, WriterProperties
from duckdb.duckdb import DuckDBPyConnection
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement
from storage_api_client import SAPIClient

from configuration import Configuration

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self._connection = self.init_connection()
        self._uc_client = None
        self.table = None
        self.stg_name = None
        self.uc_table_path = None

    def run(self):
        tables = self.get_input_tables_definitions(orphaned_manifests=True)
        files = self.get_input_files_definitions()

        if (tables and files) or (not tables and not files):
            raise Exception("Each configuration row can be mapped to either a file or a table, but not both.")

        if len(tables) > 1:
            raise UserException("Each configuration row can have only one input table")

        if self.params.destination.table_type == "native" and files:
            raise UserException("Native DBX tables support load only from tables.")

        if tables:
            self.table = tables[0]

        dest = self.params.destination
        self.uc_table_path = f"{dest.catalog}.{dest.schema_name}.{dest.table}"

        if self.params.destination.table_type == "external":
            self.write_external_table(files, tables)
        elif self.params.destination.table_type == "native":
            # used finally so even when write_native_table fails it removes stage before raising exception
            try:
                self.write_native_table()
            finally:
                if self._uc_client and not self.params.keep_stage:
                    self._drop_stage_table()

    def write_external_table(self, files, tables):
        if self.params.destination.mode.value not in ["error", "append", "overwrite"]:
            raise UserException(
                f"Unsupported mode: {self.params.destination.mode.value}."
                f" Supported modes for external tables are: append, overwrite, error."
            )

        relation = None
        if tables:
            dtypes = {key: value.data_types.get("base").dtype for key, value in self.table.schema.items()}
            s3_files = self.get_s3_paths()

            relation = self._connection.sql(f"""
            SELECT *
            FROM read_csv({s3_files}, column_names={self.table.column_names}, dtypes={dtypes})
            """)
        if files:
            files_paths = [file.full_path for file in files]
            relation = self._connection.read_parquet(files_paths)
        batches = relation.fetch_arrow_reader(batch_size=self.params.batch_size)
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
            schema_mode="merge",
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
                "merge",
            )
            line += self.params.batch_size
        self._connection.close()
        # update Unity Catalog metadata from the delta table
        if self.params.access_method == "unity_catalog":
            self._execute_query(f"MSCK REPAIR TABLE {self.uc_table_path} SYNC METADATA;")

    def _get_temp_credentials_and_region(self):
        if not self._uc_client.tables.exists(full_name=self.uc_table_path).table_exists:
            raise UserException(
                f"External table {self.uc_table_path} does not exist in Unity Catalog, please create it."
            )

        table_details = self._uc_client.tables.get(full_name=self.uc_table_path)
        table_id = table_details.table_id
        region = self._uc_client.metastores.get(table_details.metastore_id).region

        try:
            creds = self._uc_client.temporary_table_credentials.generate_temporary_table_credentials(
                operation=TableOperation.READ_WRITE, table_id=table_id
            )
            return creds, region
        except dbx_errors.platform.PermissionDenied as e:
            raise UserException(f"Permission denied: {str(e)}")

    def _build_query_create_stage(self):
        self.stg_name = f"stg_{self.environment_variables.project_id}_{self.environment_variables.config_row_id}"
        column_defs = []
        for idx in range(len(self.table.schema)):
            column_defs.append(f"_c{idx} STRING")

        columns_str = ", ".join(column_defs)
        create_stage = f"""
    CREATE OR REPLACE TABLE {self.stg_name} ({columns_str});
    """
        return create_stage

    def _drop_stage_table(self):
        self._execute_query(f"DROP TABLE IF EXISTS {self.stg_name};")

    def _build_query_load_stage(self):
        s3_files = self.get_s3_paths()
        dirname = os.path.dirname(s3_files[0])
        filenames = [os.path.basename(f) for f in s3_files]
        quoted_filenames = [f"'{file}'" for file in filenames]
        files_str = ", ".join(quoted_filenames)

        load_query = f"""
        COPY INTO {self.stg_name}
        FROM '{dirname}/' WITH (
          CREDENTIAL (AWS_ACCESS_KEY = '{self.table.s3_staging.credentials_access_key_id}',
                      AWS_SECRET_KEY = '{self.table.s3_staging.credentials_secret_access_key}',
                      AWS_SESSION_TOKEN = '{self.table.s3_staging.credentials_session_token}')
        )
        FILEFORMAT = CSV
        FILES = ({files_str})
        FORMAT_OPTIONS (
          'header' = 'false',
          'inferSchema' = 'false',
          'mergeSchema' = 'false'
        );
        """
        return load_query

    def write_native_table(self):
        """
        Write to native table by reading data from S3 by running query in DBX
        """
        if self.params.destination.mode.value not in ["append", "overwrite", "upsert"]:
            raise UserException(
                f"Unsupported mode: {self.params.destination.mode.value}."
                f" Supported modes for native tables are: append, overwrite, upsert."
            )

        if not self.params.destination.warehouse:
            raise UserException("Warehouse must be specified for native tables.")

        self._uc_client = WorkspaceClient(host=self.params.unity_catalog_url, token=self.params.unity_catalog_token)

        # Create staging table
        self._execute_query(self._build_query_create_stage())

        # Load data into staging table
        self._execute_query(self._build_query_load_stage())
        logging.info("Data loaded to staging table.")

        col_defs = []
        cast_cols = []
        cast_cols_upsert = {}

        for idx, (col_name, col_def) in enumerate(self.table.schema.items()):
            dtype = col_def.data_types["base"].dtype
            col_defs.append(f"{col_name} {dtype}")
            cast_cols.append(f"CAST(_c{idx} AS {dtype}) AS {col_name}")
            cast_cols_upsert[col_name] = f"CAST(source._c{idx} AS {dtype})"  # for upsert

        primary_keys = self.table.primary_key or []
        col_defs_str = ", ".join(col_defs)

        pk = f", PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""
        partition = (
            f"PARTITIONED BY ({', '.join(self.params.destination.partition_by)})"
            if getattr(self.params.destination, "partition_by", None)
            else ""
        )

        if self.params.destination.mode.value == "overwrite":
            self._execute_query(
                f"CREATE OR REPLACE TABLE {self.uc_table_path} ({col_defs_str} {pk}) USING DELTA {partition};"
            )
        else:
            # for append and upsert modes
            self._execute_query(
                f"CREATE TABLE IF NOT EXISTS {self.uc_table_path} ({col_defs_str} {pk}) USING DELTA {partition};"
            )

        # Write to the final table
        match self.params.destination.mode.value:
            case "overwrite":
                self._execute_query(
                    f"INSERT INTO {self.uc_table_path} SELECT {', '.join(cast_cols)} FROM {self.stg_name};"
                )

            case "append":
                self._execute_query(
                    f"INSERT INTO {self.uc_table_path} SELECT {', '.join(cast_cols)} FROM {self.stg_name};"
                )

            case "upsert":
                #  https://docs.databricks.com/aws/en/delta/merge

                if not primary_keys:
                    raise UserException("Upsert mode requires primary keys to be defined in the table schema.")

                on_clause = " AND ".join(f"target.{pk} = {cast_cols_upsert[pk]}" for pk in primary_keys)
                update_clause = ", ".join(f"target.{c} = {cast_cols_upsert[c]}" for c in self.table.schema.keys())
                column_names_str = ", ".join(self.table.schema.keys())
                cast_src_cols = ", ".join([cast_cols_upsert[col] for col in self.table.schema.keys()])

                merge_sql = f"""
                MERGE INTO {self.uc_table_path} AS target
                USING {self.stg_name} AS source
                ON {on_clause}
                WHEN MATCHED THEN
                  UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN
                  INSERT ({column_names_str})
                  VALUES ({cast_src_cols});
                """

                self._execute_query(merge_sql)

    def get_s3_paths(self):
        self._connection.execute(
            f"""
            CREATE OR REPLACE SECRET (
                TYPE S3,
                REGION '{self.table.s3_staging.region}',
                KEY_ID '{self.table.s3_staging.credentials_access_key_id}',
                SECRET '{self.table.s3_staging.credentials_secret_access_key}',
                SESSION_TOKEN '{self.table.s3_staging.credentials_session_token}'
            );
            """
        )
        # read the manifest (list of dictionaries) and extract the table file urls
        manifest = self._connection.sql(
            f"FROM read_json('s3://{self.table.s3_staging.bucket}/{self.table.s3_staging.key}')"
        ).fetchone()[0]
        files = [f.get("url") for f in manifest]
        return files

    def _execute_query(self, query):
        to_log = re.sub(r"CREDENTIAL\s+?\(.+?\)", "CREDENTIAL (--SENSITIVE--)", query, flags=re.DOTALL)
        logging.debug(f"Executing query: {to_log}")

        res = self._uc_client.statement_execution.execute_statement(
            warehouse_id=self.params.destination.warehouse,
            catalog=self.params.destination.catalog,
            schema=self.params.destination.schema_name,
            statement=query,
        )

        statement_id = res.statement_id
        while res.status.state.value in ["PENDING", "RUNNING"]:
            time.sleep(5)
            logging.debug("Waiting for creating table to complete...")
            res = self._uc_client.statement_execution.get_statement(statement_id)

        if res.status.state.value == "FAILED":
            raise UserException(f"Failed to execute query {query} : {res.status.error}")

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
                if not self.params.access_method == "unity_catalog":
                    raise UserException(f"Unknown provider: {self.params.provider}")

                self._uc_client = WorkspaceClient(
                    host=self.params.unity_catalog_url, token=self.params.unity_catalog_token
                )

                temp_creds, region = self._get_temp_credentials_and_region()
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

    @sync_action("list_warehouses")
    def list_dbx_warehouses(self):
        uc_client = WorkspaceClient(host=self.params.unity_catalog_url, token=self.params.unity_catalog_token)
        warehouses = uc_client.warehouses.list()
        return [SelectElement(value=w.id, label=w.name) for w in warehouses]

    @sync_action("list_table_columns")
    def list_table_columns(self):
        in_tables = self.configuration.tables_input_mapping

        if in_tables:
            storage_client = SAPIClient(self.environment_variables.url, self.environment_variables.token)

            table_id = self.configuration.tables_input_mapping[0].source
            columns = storage_client.get_table_detail(table_id)["columns"]
        else:
            raise UserException("Can list only columns from input tables, not files.")

        return [SelectElement(col) for col in columns]


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
