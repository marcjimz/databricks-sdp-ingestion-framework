from __future__ import annotations

import copy
from datetime import UTC, datetime

from ingestion_framework.adapters import RuntimeOptions, get_adapter
from ingestion_framework.config import load_config
from tests.conftest import DEFAULT_COLUMNS
from tests.fakes import FakeDP, FakeSpark


def _define(cfg, source="testsrc", group="g1", options=None):
    dp, spark = FakeDP(), FakeSpark()
    adapter = get_adapter(cfg.source(source).type)
    adapter.define(dp, spark, cfg, cfg.group(source, group), options or RuntimeOptions())
    return dp


def test_registers_bronze_with_two_append_flows_and_silver_auto_cdc(mini_repo):
    cfg = load_config(mini_repo(groups={"g1": ["TBL_A", "TBL_B"]}), "sandbox")
    dp = _define(cfg)

    assert sorted(dp.streaming_tables) == [
        "bronze_sandbox.testsrc.tbl_a",
        "bronze_sandbox.testsrc.tbl_b",
        "silver_sandbox.testsrc.tbl_a",
        "silver_sandbox.testsrc.tbl_b",
    ]
    flows_a = [f for f in dp.flows if f["target"] == "bronze_sandbox.testsrc.tbl_a"]
    assert [f["name"] for f in flows_a] == ["tbl_a__full_load", "tbl_a__changes"]

    cdc = next(c for c in dp.cdc_flows if c["target"] == "silver_sandbox.testsrc.tbl_a")
    assert cdc["source"] == "tbl_a__silver_source"
    assert cdc["keys"] == ["ID"]
    assert cdc["sequence_by"] == "_cdc_seq"
    assert cdc["apply_as_deletes"] == "`header__change_oper` = 'D'"
    assert cdc["except_column_list"] == ["header__change_oper", "_cdc_seq"]
    assert cdc["stored_as_scd_type"] == 1
    assert cdc["name"] == "tbl_a__apply_changes"


def test_each_flow_reads_its_own_folder_event_driven(mini_repo):
    """Guards the late-binding-closure bug: every registered function must keep its own table."""
    cfg = load_config(mini_repo(groups={"g1": ["TBL_A", "TBL_B"]}), "sandbox")
    dp = _define(cfg)
    frames = {f["name"]: f["fn"]() for f in dp.flows}

    assert frames["tbl_a__full_load"].source == "s3://bucket/landing/testsrc/g1/TBL_A"
    assert frames["tbl_b__changes"].source == "s3://bucket/landing/testsrc/g1/TBL_B__ct"
    for frame in frames.values():
        assert frame.format == "cloudFiles"
        assert frame.options["cloudFiles.useManagedFileEvents"] == "true"
        assert "header__change_seq" in frame.schema if frame.source.endswith("__ct") else True
    assert "header__change_seq" not in frames["tbl_a__full_load"].schema


def test_lookback_window_applies_to_change_files_only(mini_repo):
    cfg = load_config(mini_repo(), "sandbox")
    now = datetime(2026, 11, 16, 6, 0, tzinfo=UTC)
    dp = _define(cfg, options=RuntimeOptions(ct_lookback_days=1, now=now))
    frames = {f["name"]: f["fn"]() for f in dp.flows}

    assert frames["tbl_a__changes"].options["modifiedAfter"] == "2026-11-15T06:00:00"
    assert "modifiedAfter" not in frames["tbl_a__full_load"].options


def test_silver_view_filters_projects_and_carries_contract_expectations(mini_repo):
    cols = copy.deepcopy(DEFAULT_COLUMNS)
    cols[2]["quality"] = [
        {
            "type": "custom",
            "engine": "databricks-sdp",
            "implementation": "${column} >= 0",
            "name": "amount_ok",
            "customProperties": [{"property": "action", "value": "fail"}],
        }
    ]
    cfg = load_config(mini_repo(columns=cols), "sandbox")
    dp = _define(cfg, options=RuntimeOptions(reload_cutoffs={"silver_sandbox.testsrc.tbl_a": "2026"}))
    view = dp.views["tbl_a__silver_source"]
    frame = view["fn"]()

    assert frame.source == "bronze_sandbox.testsrc.tbl_a" and frame.kind == "table"
    where = dict(frame.ops)["where"]
    assert "<> 'B'" in where and ">= '2026'" in where
    assert dict(frame.ops)["select"][0] == "CAST(`ID` AS int) AS `ID`"
    assert view["warn"] == {"id_not_null": "`ID` IS NOT NULL"}
    assert view["fail"] == {"amount_ok": "`AMOUNT` >= 0"}
    # cutoff is per table
    assert ">=" not in dict(dp.views["tbl_b__silver_source"]["fn"]().ops)["where"]


def test_table_properties_carry_traceability_and_reset_guard(mini_repo):
    cfg = load_config(mini_repo(env_overrides={"pipelines": {"bronze_reset_allowed": False}}), "sandbox")
    dp = _define(cfg)
    bronze = dp.streaming_tables["bronze_sandbox.testsrc.tbl_a"]
    silver = dp.streaming_tables["silver_sandbox.testsrc.tbl_a"]

    assert bronze["table_properties"]["pipelines.reset.allowed"] == "false"
    assert bronze["table_properties"]["ingest.pipeline"] == "ingest-testsrc-g1-sandbox"
    assert silver["table_properties"]["delta.enableChangeDataFeed"] == "true"
    assert silver["cluster_by_auto"] is True
    assert "pipelines.reset.allowed" not in silver["table_properties"]


def test_scd2_append_only_and_bronze_only_tables(mini_repo):
    cfg = load_config(
        mini_repo(
            groups={"g1": ["TBL_A", "TBL_B", "TBL_C"]},
            table_overrides={
                "TBL_A": {"scd_type": 2},
                "TBL_B": {"append_only": True},
                "TBL_C": {"silver": False},
            },
        ),
        "sandbox",
    )
    dp = _define(cfg)

    assert next(c for c in dp.cdc_flows if c["target"].endswith("tbl_a"))["stored_as_scd_type"] == 2
    assert not [c for c in dp.cdc_flows if c["target"].endswith(("tbl_b", "tbl_c"))]
    assert any(f["target"] == "silver_sandbox.testsrc.tbl_b" for f in dp.flows)
    assert "silver_sandbox.testsrc.tbl_c" not in dp.streaming_tables


def test_datasets_describe_every_table_the_group_owns(mini_repo):
    cfg = load_config(mini_repo(groups={"g1": ["TBL_A"]}), "sandbox")
    adapter = get_adapter("qlik_cdc_files")
    ds = adapter.datasets(cfg, cfg.group("testsrc", "g1"))

    assert [(d.layer, d.fqn) for d in ds] == [
        ("bronze", "bronze_sandbox.testsrc.tbl_a"),
        ("silver", "silver_sandbox.testsrc.tbl_a"),
    ]
