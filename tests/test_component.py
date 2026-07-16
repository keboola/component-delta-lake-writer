import unittest
import mock
import os
from types import SimpleNamespace as NS

from freezegun import freeze_time

from keboola.component.exceptions import UserException

from component import Component
from configuration import Configuration


def make_component(**param_overrides):
    """Build a Component instance without running ComponentBase.__init__ (no datadir needed)."""
    params = {
        "access_method": "unity_catalog",
        "unity_catalog_url": "https://workspace.example.com",
        "destination": {},
    }
    params.update(param_overrides)
    comp = Component.__new__(Component)
    comp.params = Configuration(**params)
    return comp


def mock_conn(manifest_rows):
    """DuckDB connection stub whose read_json returns a single row holding the manifest list."""
    conn = mock.MagicMock()
    conn.sql.return_value.fetchone.return_value = [manifest_rows]
    return conn


def executed_sql(conn):
    return " ".join(str(call.args[0]) for call in conn.execute.call_args_list)


class TestComponent(unittest.TestCase):
    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    # --- auth selection ---------------------------------------------------------------

    @mock.patch("component.WorkspaceClient")
    def test_get_workspace_client_pat(self, wc):
        comp = make_component(auth_type="pat", **{"#unity_catalog_token": "dapi-token"})
        comp._get_workspace_client()
        wc.assert_called_once_with(host="https://workspace.example.com", token="dapi-token")

    @mock.patch("component.WorkspaceClient")
    def test_get_workspace_client_service_principal(self, wc):
        comp = make_component(
            auth_type="service_principal",
            unity_catalog_client_id="client-id",
            **{"#unity_catalog_client_secret": "client-secret"},
        )
        comp._get_workspace_client()
        wc.assert_called_once_with(
            host="https://workspace.example.com",
            client_id="client-id",
            client_secret="client-secret",
        )

    # --- input file staging (S3 / Azure Blob) -----------------------------------------

    def test_get_staging_files_s3(self):
        comp = make_component()
        s3 = NS(
            region="us-east-1",
            bucket="b",
            key="k/manifest",
            credentials_access_key_id="ak",
            credentials_secret_access_key="sk",
            credentials_session_token="st",
        )
        comp.table = NS(abs_staging=None, s3_staging=s3)
        comp._connection = mock_conn([{"url": "s3://b/x/part-0.csv"}, {"url": "s3://b/x/part-1.csv"}])

        files = comp.get_staging_files()

        self.assertEqual(files, ["s3://b/x/part-0.csv", "s3://b/x/part-1.csv"])
        self.assertIn("TYPE S3", executed_sql(comp._connection))
        self.assertIn("s3://b/k/manifest", comp._connection.sql.call_args[0][0])

    def test_get_staging_files_abs(self):
        comp = make_component()
        abs_stg = NS(
            container="mycontainer",
            name="627703071.csv.gzmanifest",
            credentials_sas_connection_string=(
                "BlobEndpoint=https://acct.blob.core.windows.net;SharedAccessSignature=sv=X&sig=Y"
            ),
        )
        comp.table = NS(abs_staging=abs_stg, s3_staging=None)
        comp._connection = mock_conn(
            [
                {"url": "azure://acct.blob.core.windows.net/mycontainer/path/part-0.csv"},
                {"url": "azure://acct.blob.core.windows.net/mycontainer/path/part-1.csv"},
            ]
        )

        files = comp.get_staging_files()

        # slice URLs normalized to DuckDB az://<container>/<path> form
        self.assertEqual(
            files,
            ["az://mycontainer/path/part-0.csv", "az://mycontainer/path/part-1.csv"],
        )
        executed = executed_sql(comp._connection)
        self.assertIn("TYPE AZURE", executed)
        self.assertIn("azure_transport_option_type", executed)
        # manifest read from az://<container>/<name>
        self.assertIn("az://mycontainer/627703071.csv.gzmanifest", comp._connection.sql.call_args[0][0])

    def test_get_staging_files_none_raises(self):
        comp = make_component()
        comp.table = NS(abs_staging=None, s3_staging=None)
        with self.assertRaises(UserException):
            comp.get_staging_files()

    # --- native COPY INTO stage query --------------------------------------------------

    def test_build_s3_load_stage(self):
        comp = make_component()
        comp.stg_name = "stg_1_1"
        s3 = NS(
            credentials_access_key_id="ak",
            credentials_secret_access_key="sk",
            credentials_session_token="st",
        )
        comp.table = NS(abs_staging=None, s3_staging=s3)

        query = comp._build_s3_load_stage(["s3://b/x/part-0.csv", "s3://b/x/part-1.csv"])

        self.assertIn("AWS_ACCESS_KEY = 'ak'", query)
        self.assertIn("FROM 's3://b/x/'", query)
        self.assertIn("'part-0.csv'", query)

    def test_build_abs_load_stage(self):
        comp = make_component()
        comp.stg_name = "stg_1_1"
        abs_stg = NS(
            container="mycontainer",
            credentials_sas_connection_string=(
                "BlobEndpoint=https://acct.blob.core.windows.net;SharedAccessSignature=sv=X&sig=Y"
            ),
        )
        comp.table = NS(abs_staging=abs_stg, s3_staging=None)

        query = comp._build_abs_load_stage(
            ["az://mycontainer/path/part-0.csv", "az://mycontainer/path/part-1.csv"]
        )

        self.assertIn("AZURE_SAS_TOKEN = 'sv=X&sig=Y'", query)
        self.assertIn("FROM 'abfss://mycontainer@acct.dfs.core.windows.net/path/'", query)
        self.assertIn("'part-0.csv'", query)

    # --- ABS helpers -------------------------------------------------------------------

    def test_parse_abs_connection_string(self):
        # real Keboola ABS staging connection string (URL-encoded sig, multiple SAS params)
        conn_str = (
            "BlobEndpoint=https://kbcfshc7chguaeh2km.blob.core.windows.net;"
            "SharedAccessSignature=sv=2017-11-09&sr=c&st=2026-07-16T12:50:12Z&se=2026-07-17T00:50:12Z"
            "&sp=rl&sig=Td2bBoGTGuBKDlBzng%2B2JHGzx%2BP34adli7LG%2FMg2CJY%3D"
        )
        account, sas = Component._parse_abs_connection_string(conn_str)
        self.assertEqual(account, "kbcfshc7chguaeh2km")
        self.assertEqual(
            sas,
            "sv=2017-11-09&sr=c&st=2026-07-16T12:50:12Z&se=2026-07-17T00:50:12Z"
            "&sp=rl&sig=Td2bBoGTGuBKDlBzng%2B2JHGzx%2BP34adli7LG%2FMg2CJY%3D",
        )

    def test_abs_relative_path_variants(self):
        cases = [
            "az://cont/a/b.csv",
            "azure://acct.blob.core.windows.net/cont/a/b.csv",
            "https://acct.blob.core.windows.net/cont/a/b.csv",
        ]
        for url in cases:
            self.assertEqual(Component._abs_relative_path(url, "cont"), "a/b.csv", msg=url)


if __name__ == "__main__":
    unittest.main()
