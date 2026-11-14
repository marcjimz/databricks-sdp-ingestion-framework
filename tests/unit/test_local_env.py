from __future__ import annotations

import yaml

from ingestion_framework.prereqs.local_env import local_env_from_output, merge_local_env

OUTPUT = {
    "workspace_host": "https://adb-1.2.azuredatabricks.net",
    "account_id": "acct-1",
    "landing_facets": "abfss://landing@a.dfs.core.windows.net/facets",
    "storage": {
        "create": True,
        "cloud": "azure",
        "credential_name": "c",
        "external_location_name": "l",
        "access_connector_id": "/sub/ac",
        "resource_group": "rg",
        "subscription_id": "s1",
        "url": "abfss://landing@a.dfs.core.windows.net/",
        "catalog_storage_root": "abfss://catalogs@a.dfs.core.windows.net/",
    },
}


def test_terraform_output_maps_to_env_overrides():
    doc = local_env_from_output(OUTPUT)
    assert doc["workspace_host"] == "https://adb-1.2.azuredatabricks.net"
    assert doc["landing"] == {"facets": "abfss://landing@a.dfs.core.windows.net/facets"}
    assert doc["storage"]["cloud"] == "azure" and doc["storage"]["create"] is True
    assert doc["governance"] == {"account_id": "acct-1"}


def test_merge_keeps_personal_values_and_replaces_storage(tmp_path):
    path = tmp_path / "sandbox.local.yml"
    path.write_text(yaml.safe_dump({"alerts": {"notify": {"emails": ["me@x"]}}, "storage": {"iam_role_arn": "old"}}))
    merge_local_env(path, local_env_from_output(OUTPUT))
    doc = yaml.safe_load(path.read_text())

    assert doc["alerts"]["notify"]["emails"] == ["me@x"]
    assert doc["storage"]["access_connector_id"] == "/sub/ac"
    assert "iam_role_arn" not in doc["storage"]  # storage is replaced wholesale, never mixed across clouds
