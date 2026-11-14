"""Durable enforcement tests: principles that must hold for every source, group, table and env.

These run against the REAL repo configuration for every environment, and against a dummy source
adapter to prove that any future source type (e.g. a Lakeflow Connect connector) inherits the
same governance automatically. If one fails, a change broke a framework guarantee.
"""

from __future__ import annotations

from typing import Any

import pytest

from ingestion_framework.adapters import (
    Dataset,
    SourceAdapter,
    get_adapter,
    register_adapter,
    unregister_adapter,
)
from ingestion_framework.bundle import generate
from ingestion_framework.config import available_envs, load_config
from tests.conftest import REPO_ROOT

pytestmark = pytest.mark.enforcement
ENVS = available_envs(REPO_ROOT)


def _spec(env: str) -> dict[str, Any]:
    return generate(load_config(REPO_ROOT, env), git_commit="test")


@pytest.mark.parametrize("env", ENVS)
def test_every_pipeline_has_event_log_tags_and_failure_notifications_wiring(env):
    cfg = load_config(REPO_ROOT, env)
    for key, p in _spec(env)["pipelines"].items():
        assert p.get("event_log", {}).get("schema") == cfg.ops_schema, key
        for logical in (
            "ingest_pipeline",
            "ingest_source",
            "ingest_config",
            "ingest_code",
            "ingest_git_commit",
        ):
            assert cfg.tag_key(logical) in p["tags"], (key, logical)
        assert p["tags"][cfg.tag_key("ingest_pipeline")] == p["name"], key
        if cfg.env.alert_emails:
            assert p["notifications"][0]["alerts"] == [
                "on-update-failure",
                "on-update-fatal-failure",
                "on-flow-failure",
            ], key


@pytest.mark.parametrize("env", ENVS)
def test_every_source_schema_is_declared_and_dqm_monitored(env):
    cfg = load_config(REPO_ROOT, env)
    spec = _spec(env)
    for src in cfg.sources_in_scope():
        for layer in ("bronze", "silver"):
            assert f"schema_{layer}_{src.source}" in spec["schemas"]
            if cfg.env.dqm.get("enabled", True):
                assert f"dqm_{layer}_{src.source}" in spec["data_quality_monitors"]
    assert cfg.env.dqm.get("enabled", True), f"{env}: DQM must stay enabled"


@pytest.mark.parametrize("env", ENVS)
def test_every_env_declares_the_required_alert_set(env):
    cfg = load_config(REPO_ROOT, env)
    required = cfg.framework.section("alerts")["required"]
    spec = _spec(env)
    assert cfg.env.alerts.get("enabled", True), f"{env}: alerts must stay enabled"
    for name in required:
        assert f"alert_{name}" in spec["alerts"], (env, name)


@pytest.mark.parametrize("env", ENVS)
def test_every_triggered_cadence_respects_the_file_events_gap(env):
    cfg = load_config(REPO_ROOT, env)
    limit = float(cfg.framework.limits["max_triggered_gap_days"])
    for g in cfg.groups_in_scope():
        cadence = cfg.cadence_for(g.source, g.group)
        gap = cadence.max_gap_days()
        if cadence.mode == "cron":
            assert gap is not None, f"{env}: cron profile '{cadence.name}' must declare max_gap_days"
        if gap is not None:
            assert gap <= limit, f"{env}/{g.key}: '{cadence.name}' idles {gap}d > {limit}d (directory-listing fallback)"


@pytest.mark.parametrize("env", ENVS)
def test_ingestion_is_event_driven_everywhere(env):
    from ingestion_framework.adapters.qlik_cdc_files import builders as b

    cfg = load_config(REPO_ROOT, env)
    assert cfg.framework.section("discovery")["mode"] == "file_events"
    for src in cfg.sources_in_scope():
        if src.type == "qlik_cdc_files":
            opts = b.reader_options(src.settings["qlik"])
            assert opts["cloudFiles.useManagedFileEvents"] == "true", src.source
            assert "cloudFiles.useNotifications" not in opts, src.source
            assert "cloudFiles.backfillInterval" not in opts, src.source


def test_prod_is_protected():
    cfg = load_config(REPO_ROOT, "prod")
    spec = _spec("prod")
    assert cfg.env.pipelines["prevent_destroy"] is True
    assert cfg.env.pipelines["bronze_reset_allowed"] is False
    assert not cfg.env.seed_enabled
    for kind in ("pipelines", "schemas"):
        for key, res in spec[kind].items():
            assert res.get("lifecycle") == {"prevent_destroy": True}, (kind, key)


def test_seed_job_never_outside_sandbox():
    for env in ENVS:
        if env != "sandbox":
            assert "job_seed" not in _spec(env)["jobs"], env


class _DummyManagedAdapter(SourceAdapter):
    """Stands in for any future source type, e.g. a Lakeflow Connect connector."""

    type = "dummy_managed"
    runs_framework_code = False

    def datasets(self, cfg, group):
        return [Dataset("bronze", cfg.table_fqn("bronze", t.source, t.name), t) for t in group.tables]

    def pipeline_body(self, cfg, group):
        return {
            "catalog": cfg.catalog("bronze"),
            "schema": cfg.schema(group.source),
            "ingestion_definition": {"connection_name": "conn", "objects": []},
        }


def test_a_new_source_type_inherits_all_governance(mini_repo):
    # Managed connectors have no landing folder to watch, so they use a scheduled cadence.
    root = mini_repo(env_overrides={"cadence": {"default": "every_15_minutes"}})
    src_file = root / "sources" / "testsrc" / "source.yml"
    src_file.write_text(src_file.read_text().replace("type: qlik_cdc_files", "type: dummy_managed"))
    register_adapter(_DummyManagedAdapter())
    try:
        cfg = load_config(root, "sandbox")
        assert get_adapter("dummy_managed").runs_framework_code is False
        spec = generate(cfg, git_commit="test")
        p = spec["pipelines"]["pipeline_testsrc_g1"]
        assert p["ingestion_definition"]["connection_name"] == "conn"
        assert p["event_log"]["schema"] == "ingest_ops"
        assert p["tags"]["ingest_pipeline"] == "ingest-testsrc-g1-sandbox"
        assert p["notifications"]
        assert "dqm_bronze_testsrc" in spec["data_quality_monitors"]
        assert "alert_undeclared_objects" in spec["alerts"]
        assert "job_testsrc_g1_trigger" in spec["jobs"]
    finally:
        unregister_adapter("dummy_managed")


def test_bundle_sync_never_excludes_framework_code():
    """The bundle CLI matches sync.exclude patterns at ANY depth (a leading "/" does not anchor them), so
    `contracts/**` also dropped src/ingestion_framework/contracts/ from the deployment. No exclude may share
    a name with a directory under src/."""
    import yaml

    excludes = (yaml.safe_load((REPO_ROOT / "databricks.yml").read_text()).get("sync") or {}).get("exclude") or []
    src_dirs = {p.name for p in (REPO_ROOT / "src").rglob("*") if p.is_dir() and p.name != "__pycache__"}
    for pattern in excludes:
        first = pattern.lstrip("/").split("/", 1)[0]
        assert first not in src_dirs, f"sync.exclude '{pattern}' would also exclude src/**/{first}/"


def test_local_environment_overrides_are_deployed_with_the_bundle():
    """Runtime must load the same config the bundle was generated from; <env>.local.yml is gitignored,
    so it has to be explicitly included in the bundle sync."""
    import yaml

    include = (yaml.safe_load((REPO_ROOT / "databricks.yml").read_text()).get("sync") or {}).get("include") or []
    assert "conf/environments/*.local.yml" in include
