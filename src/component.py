"""
Template Component main class.

"""

import logging
import os
import time
import duckdb
from deltalake import write_deltalake, WriterProperties
from duckdb.duckdb import DuckDBPyConnection
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import Configuration

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self._connection = self.init_connection()

    def run(self):
        """
        Main execution code
        """

        tables = self.get_input_tables_definitions()
        files = self.get_input_files_definitions()

        if (tables and files) or (not tables and not files):
            raise Exception("Each configuration row can be mapped to either a file or a table, but not both.")

        if len(tables) > 1:
            raise UserException("Each configuration row can have only one input table")

        relation = None
        if tables:
            table = tables[0]
            if table.is_sliced:
                path = f"{table.full_path}/*.csv"
            else:
                path = table.full_path

            dtypes = {key: value.data_types.get("base").dtype for key, value in table.schema.items()}
            relation = self._connection.read_csv(path, dtype=dtypes)

        if files:
            files_paths = [file.full_path for file in files]
            relation = self._connection.read_parquet(files_paths)

        batches = relation.fetch_arrow_reader(batch_size=self.params.batch_size)

        storage_options = {
            "timeout": "3600s",
            "max_retries": "2",
        }

        if self.params.provider == "abs":
            uri = f"az://{self.params.destination.container_name}/{self.params.destination.blob_name}"
            storage_options |= {
                "azure_storage_account_name": self.params.abs_account_name,
                "azure_storage_sas_token": self.params.abs_sas_token,
            }

        elif self.params.provider == "s3":
            uri = f"s3://{self.params.destination.container_name}/{self.params.destination.blob_name}"
            storage_options |= {
                "aws_region": self.params.aws_region,
                "aws_access_key_id": self.params.aws_key_id,
                "aws_secret_access_key": self.params.aws_key_secret,
            }

        elif self.params.provider == "gcs":
            uri = f"gs://{self.params.destination.container_name}/{self.params.destination.blob_name}"
            storage_options |= {"google_service_account_key": self.params.gcp_service_account_key}

        else:
            raise UserException(f"Unknown provider: {self.params.provider}")

        writer_properties = WriterProperties(data_page_size_limit=8 * 1024 * 1024,
                                             compression=self.params.destination.compression)

        line = self.params.batch_size
        logging.info(f"Writing records {line} - {line + self.params.batch_size}")
        self.write_batch(
            uri,
            next(batches),
            self.params.destination.mode.value,
            storage_options,
            writer_properties,
            self.params.destination.partition_by,
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

    @staticmethod
    def write_batch(table_or_uri, data, mode, storage_options, writer_properties, partition_by=None):
        start = time.time()
        write_deltalake(
            table_or_uri=table_or_uri,
            data=data,
            mode=mode,
            storage_options=storage_options,
            writer_properties=writer_properties,
            partition_by=partition_by,
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
