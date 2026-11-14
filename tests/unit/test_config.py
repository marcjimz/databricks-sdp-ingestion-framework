from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ingestion_framework.config import ConfigError, load_config


def test_loads_env_sources_groups_and_tables(mini_repo):
    root = mini_repo(groups={"g1": ["TBL_A", "TBL_B"], "g2": ["TBL_C"]})
    cfg = load_config(root, "sandbox")

    assert cfg.env.name == "sandbox"
    assert [s.source for s in cfg.sources] == ["testsrc"]
    groups = cfg.groups_in_scope()
    assert [(g.source, g.group) for g in groups] == [("testsrc", "g1"), ("testsrc", "g2")]
    assert [t.table for t in groups[0].tables] == ["TBL_A", "TBL_B"]
    assert groups[0].tables[0].name == "tbl_a"


def test_table_inherits_source_defaults_and_reads_keys_from_contract(mini_repo):
    root = mini_repo(table_overrides={"TBL_B": {"scd_type": 2}})
    cfg = load_config(root, "sandbox")
    a, b = cfg.groups_in_scope()[0].tables

    assert a.scd_type == 1 and b.scd_type == 2
    assert a.silver is True
    assert a.classification == "phi"
    assert a.contract.keys == ("ID",)
    assert [c.name for c in a.contract.columns] == ["ID", "NAME", "AMOUNT"]
    assert a.config_path.as_posix().endswith("groups/g1/tables/tbl_a/table.yml")
    assert a.contract_path.as_posix().endswith("groups/g1/tables/tbl_a/contract.odcs.yaml")


def test_local_override_file_is_deep_merged(mini_repo):
    root = mini_repo()
    (root / "conf" / "environments" / "sandbox.local.yml").write_text(
        yaml.safe_dump({"landing": {"testsrc": "s3://mine/landing"}, "alerts": {"notify": {"emails": ["me@x"]}}})
    )
    cfg = load_config(root, "sandbox")

    assert cfg.landing_root("testsrc") == "s3://mine/landing"
    assert cfg.env.alert_emails == ("me@x",)
    assert cfg.env.alerts["warehouse_name"] == "wh"  # untouched keys survive the merge


def test_naming_and_cadence_resolution(mini_repo):
    root = mini_repo(
        env="prod",
        env_overrides={"cadence": {"default": "continuous", "overrides": {"testsrc/g1": "on_arrival"}}},
        groups={"g1": ["TBL_A"], "g2": ["TBL_B"]},
    )
    cfg = load_config(root, "prod")

    assert cfg.pipeline_name("testsrc", "g1") == "ingest-testsrc-g1-prod"
    assert cfg.table_fqn("silver", "testsrc", "tbl_a") == "silver_prod.testsrc.tbl_a"
    assert cfg.ops_schema_fqn == "bronze_prod.ingest_ops"
    assert cfg.event_log_fqn("testsrc", "g1") == "bronze_prod.ingest_ops.event_log_testsrc_g1"
    assert cfg.cadence_for("testsrc", "g1").mode == "file_arrival"
    assert cfg.cadence_for("testsrc", "g2").mode == "continuous"


def test_tag_prefix_applies_to_every_key(mini_repo):
    root = mini_repo(env_overrides={"governance": {"tag_prefix": "sdpif_"}})
    cfg = load_config(root, "sandbox")

    assert cfg.tag_key("ingest_pipeline") == "sdpif_ingest_pipeline"


def test_scope_filters_groups_and_tables(mini_repo):
    root = mini_repo(
        groups={"g1": ["TBL_A", "TBL_B"], "g2": ["TBL_C"]},
        env_overrides={"scope": {"groups": ["testsrc/g1"], "tables": ["testsrc/g1/TBL_B"]}},
    )
    cfg = load_config(root, "sandbox")
    groups = cfg.groups_in_scope()

    assert [g.group for g in groups] == ["g1"]
    assert [t.table for t in groups[0].tables] == ["TBL_B"]


def test_disabled_tables_are_excluded(mini_repo):
    root = mini_repo(table_overrides={"TBL_A": {"enabled": False}})
    cfg = load_config(root, "sandbox")

    assert [t.table for t in cfg.groups_in_scope()[0].tables] == ["TBL_B"]


def test_unknown_env_and_unknown_cadence_fail_clearly(mini_repo):
    root = mini_repo(env_overrides={"cadence": {"default": "hourly-ish"}})
    with pytest.raises(ConfigError, match="no environment file"):
        load_config(root, "nope")
    with pytest.raises(ConfigError, match="unknown cadence profile 'hourly-ish'"):
        load_config(root, "sandbox").cadence_for("testsrc", "g1")


def test_missing_contract_is_a_config_error(mini_repo):
    root = mini_repo()
    Path(root, "sources/testsrc/groups/g1/tables/tbl_a/contract.odcs.yaml").unlink()
    with pytest.raises(ConfigError, match=r"tbl_a/contract\.odcs\.yaml"):
        load_config(root, "sandbox")


def test_group_folder_without_group_file_fails_loudly(mini_repo):
    """A silently skipped group would delete its pipeline on the next deploy."""
    root = mini_repo()
    Path(root, "sources/testsrc/groups/g1/group.yml").unlink()
    with pytest.raises(ConfigError, match=r"g1/group\.yml"):
        load_config(root, "sandbox")


def test_real_repo_config_loads_for_every_environment():
    from tests.conftest import REPO_ROOT

    for env in ("sandbox", "dev", "tst", "prod"):
        cfg = load_config(REPO_ROOT, env)
        assert cfg.groups_in_scope(), env
