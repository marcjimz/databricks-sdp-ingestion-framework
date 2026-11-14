"""Shared fixtures: a throwaway mini-repo built from the real framework defaults."""

from __future__ import annotations

import copy
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]


def _write(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False))


def contract_doc(
    table: str,
    columns: list[dict[str, Any]],
    *,
    quality: list[dict[str, Any]] | None = None,
    classification: str = "phi",
) -> dict[str, Any]:
    """Minimal-but-complete ODCS v3.2.0 contract in the framework's house profile."""
    obj: dict[str, Any] = {
        "name": table.lower(),
        "physicalName": table,
        "logicalType": "object",
        "physicalType": "table",
        "description": f"{table} test table",
        "properties": copy.deepcopy(columns),  # never let a test mutate shared fixtures
    }
    if quality:
        obj["quality"] = copy.deepcopy(quality)
    return {
        "apiVersion": "v3.2.0",
        "kind": "DataContract",
        "id": f"urn:ingest:testsrc:{table.lower()}",
        "name": f"testsrc.{table.lower()}",
        "version": "1.0.0",
        "status": "active",
        "domain": "testing",
        "team": {"name": "data-platform", "members": [{"username": "owner@example.com", "role": "owner"}]},
        "description": {"purpose": "test"},
        "schema": [obj],
        "slaProperties": [{"property": "latency", "value": 60, "unit": "m"}],
        "customProperties": [{"property": "dataClassification", "value": classification}],
    }


DEFAULT_COLUMNS = [
    {
        "name": "ID",
        "logicalType": "integer",
        "physicalType": "int",
        "primaryKey": True,
        "primaryKeyPosition": 1,
        "required": True,
    },
    {"name": "NAME", "logicalType": "string", "physicalType": "string"},
    {"name": "AMOUNT", "logicalType": "number", "physicalType": "decimal(18,2)"},
]


@pytest.fixture
def mini_repo(tmp_path: Path) -> Callable[..., Path]:
    """Factory: build a repo root with real framework.yml and a configurable test source."""

    def build(
        *,
        env: str = "sandbox",
        env_overrides: dict[str, Any] | None = None,
        groups: dict[str, list[str]] | None = None,
        columns: list[dict[str, Any]] | None = None,
        quality: list[dict[str, Any]] | None = None,
        table_overrides: dict[str, dict[str, Any]] | None = None,
    ) -> Path:
        root = tmp_path / "repo"
        (root / "conf" / "environments").mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO_ROOT / "conf" / "framework.yml", root / "conf" / "framework.yml")
        shutil.copy(REPO_ROOT / "conf" / "prerequisites.yml", root / "conf" / "prerequisites.yml")
        env_doc: dict[str, Any] = {
            "env": env,
            "short": "sbx" if env == "sandbox" else env,
            "description": "test env",
            "workspace_host": "",
            "deployer": {"service_principal": "sp-app-id"},
            "catalogs": {"bronze": f"bronze_{env}", "silver": f"silver_{env}"},
            "landing": {"testsrc": "s3://bucket/landing/testsrc"},
            "cadence": {"default": "on_arrival", "overrides": {}},
            "scope": {"sources": [], "groups": [], "tables": []},
            "governance": {
                "tag_prefix": "",
                "governed_tags": True,
                "reader_groups": [],
                "admin_groups": [],
            },
            "dqm": {"enabled": True, "notify": {"emails": ["dq@example.com"], "destination_ids": []}},
            "alerts": {
                "enabled": True,
                "warehouse_name": "wh",
                "schedule": "0 0/15 * * * ?",
                "timezone": "UTC",
                "notify": {"emails": ["ops@example.com"], "destination_ids": []},
                "dqm_system_table_alert": False,
            },
            "pipelines": {
                "serverless": True,
                "channel": "CURRENT",
                "prevent_destroy": False,
                "bronze_reset_allowed": True,
            },
            "seed": {"enabled": False},
        }
        _deep_update(env_doc, env_overrides or {})
        _write(root / "conf" / "environments" / f"{env}.yml", env_doc)

        src_dir = root / "sources" / "testsrc"
        _write(
            src_dir / "source.yml",
            {
                "source": "testsrc",
                "type": "qlik_cdc_files",
                "description": "test source",
                "qlik": yaml.safe_load((REPO_ROOT / "sources" / "facets" / "source.yml").read_text())["qlik"],
                "defaults": {"scd_type": 1, "silver": True, "classification": "phi"},
            },
        )
        for group, tables in (groups or {"g1": ["TBL_A", "TBL_B"]}).items():
            gdir = src_dir / "groups" / group
            _write(
                gdir / "group.yml",
                {"group": group, "description": f"{group} desc", "domain": group, "landing_folder": group},
            )
            for table in tables:
                tdoc: dict[str, Any] = {"table": table, "enabled": True}
                tdoc.update((table_overrides or {}).get(table, {}))
                tdir = gdir / "tables" / table.lower()
                _write(tdir / "table.yml", tdoc)
                _write(
                    tdir / "contract.odcs.yaml",
                    contract_doc(table, columns or DEFAULT_COLUMNS, quality=quality),
                )
        return root

    return build


def _deep_update(base: dict[str, Any], extra: dict[str, Any]) -> None:
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)
        else:
            base[key] = value
