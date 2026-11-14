from __future__ import annotations

from datetime import UTC, datetime

import yaml

from ingestion_framework.adapters.qlik_cdc_files import builders as b
from ingestion_framework.config import Column
from tests.conftest import REPO_ROOT

QLIK = yaml.safe_load((REPO_ROOT / "sources" / "facets" / "source.yml").read_text())["qlik"]
COLS = (
    Column("ID", "integer", "int", True, 1),
    Column("NAME", "string", None, False, None),
    Column("AMT", "number", "decimal(18,2)", False, None),
    Column("BORN", "date", None, False, None),
)


def test_paths_follow_qlik_layout():
    assert b.full_load_path("s3://b/landing/facets", "claims", "CMC_CLCL_CLAIM") == (
        "s3://b/landing/facets/claims/CMC_CLCL_CLAIM"
    )
    assert b.changes_path("s3://b/landing/facets", "claims", "CMC_CLCL_CLAIM", "__ct") == (
        "s3://b/landing/facets/claims/CMC_CLCL_CLAIM__ct"
    )


def test_cdc_seq_sorts_full_loads_by_file_time_and_before_every_change():
    full = b.cdc_seq_full_load_expr(35)
    change = b.cdc_seq_change_expr("header__change_seq", 35)
    # rank 0 + file time: a later reload's snapshot outranks an earlier one, but never a change row
    assert full == ("concat('0', date_format(_metadata.file_modification_time, 'yyyyMMddHHmmssSSS'), repeat('0', 18))")
    assert change == "concat('1', coalesce(`header__change_seq`, repeat('0', 35)))"


def test_bronze_columns_are_identical_and_ordered_for_both_flows():
    full = b.bronze_select_full_load(COLS, QLIK)
    changes = b.bronze_select_changes(COLS, QLIK)
    alias = lambda exprs: [e.rsplit(" AS ", 1)[-1] for e in exprs]  # noqa: E731

    assert alias(full) == alias(changes)
    assert alias(full)[:4] == ["`ID`", "`NAME`", "`AMT`", "`BORN`"]
    assert "'L' AS `header__change_oper`" in full
    assert "CAST(NULL AS timestamp) AS `header__timestamp`" in full
    assert "`header__change_oper` AS `header__change_oper`" in changes
    assert "'FULL' AS `_load_type`" in full and "'CDC' AS `_load_type`" in changes
    assert "_metadata.file_path AS `_ingest_file`" in full
    assert alias(full)[-1] == "`_rescued_data`"


def test_reader_schema_comes_from_contract_plus_headers_on_changes():
    full_ddl = b.reader_schema_ddl(COLS, QLIK, include_headers=False)
    ct_ddl = b.reader_schema_ddl(COLS, QLIK, include_headers=True)

    assert full_ddl == "`ID` int, `NAME` string, `AMT` decimal(18,2), `BORN` date"
    assert ct_ddl.startswith(full_ddl + ", `header__change_seq` string")
    assert "`header__timestamp` timestamp" in ct_ddl


def test_reader_options_are_event_driven_and_rescue_unknown_columns():
    opts = b.reader_options(QLIK)

    assert opts["cloudFiles.useManagedFileEvents"] == "true"
    assert opts["cloudFiles.format"] == "parquet"
    assert opts["cloudFiles.schemaEvolutionMode"] == "rescue"
    assert opts["rescuedDataColumn"] == "_rescued_data"
    assert opts["pathGlobFilter"] == "*.parquet"
    assert "cloudFiles.useNotifications" not in opts
    assert "modifiedAfter" not in opts


def test_lookback_window_only_sets_modified_after():
    now = datetime(2026, 11, 16, 6, 0, tzinfo=UTC)
    opts = b.reader_options(QLIK, lookback_days=1, now=now)
    assert opts["modifiedAfter"] == "2026-11-15T06:00:00"


def test_silver_source_drops_before_images_and_honours_reload_cutoff():
    assert b.silver_source_filter(QLIK) == "`header__change_oper` <> 'B'"
    # after a reload at the cutoff, both the old snapshot and older changes are ignored
    assert b.silver_source_filter(QLIK, reload_cutoff="20261113000000") == (
        "`header__change_oper` <> 'B' AND ("
        "(`_load_type` = 'FULL' AND date_format(`_file_modification_time`, 'yyyyMMddHHmmss') >= '20261113000000')"
        " OR (`_load_type` = 'CDC' AND `header__change_seq` >= '20261113000000'))"
    )


def test_silver_projection_casts_to_contract_types_and_keeps_cdc_columns():
    proj = b.silver_projection(COLS, QLIK)
    assert proj[:2] == ["CAST(`ID` AS int) AS `ID`", "CAST(`NAME` AS string) AS `NAME`"]
    assert proj[-2:] == ["`header__change_oper`", "`_cdc_seq`"]
    assert b.silver_except_columns(QLIK) == ["header__change_oper", "_cdc_seq"]
    assert b.delete_condition(QLIK) == "`header__change_oper` = 'D'"


def test_logical_types_map_when_physical_type_missing():
    assert b.spark_type(Column("X", "integer", None, False, None)) == "bigint"
    assert b.spark_type(Column("X", "timestamp", None, False, None)) == "timestamp"
    assert b.spark_type(Column("X", "number", None, False, None)) == "double"
