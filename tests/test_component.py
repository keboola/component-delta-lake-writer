import unittest
import mock
import os

from freezegun import freeze_time

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


if __name__ == "__main__":
    unittest.main()
