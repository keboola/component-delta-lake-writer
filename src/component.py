"""
Template Component main class.

"""

import logging
import os

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

        arrow_batches = relation.fetch_arrow_reader(batch_size=self.params.batch_size)

        storage_options = {
            "azure_storage_account_name": self.params.account_name,
            "azure_storage_sas_token": self.params.sas_token,
            "timeout": "55s",
            "max_retries": "1",
        }

        uri = f"az://{self.params.destination.container_name}/{self.params.destination.blob_name}"

        writer_properties = WriterProperties(
            write_batch_size=10000,
            # data_page_size_limit=8 * 1024 * 1024,
            # dictionary_page_size_limit=8 * 1024 * 1024,
        )


        write_deltalake(
            table_or_uri=uri,
            data=arrow_batches,
            storage_options=storage_options,
            partition_by=self.params.destination.partition_by,
            mode=self.params.destination.mode.value,
            writer_properties=writer_properties,
            # target_file_size=64 * 1024 * 1024,
        )

        self._connection.close()

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
