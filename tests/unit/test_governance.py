from __future__ import annotations

from ingestion_framework.config import load_config
from ingestion_framework.governance import governed_tag_values, registry_rows
from ingestion_framework.governance.job import (
    desired_table_tags,
    event_log_view_sql,
    tag_statements,
    undeclared_tables,
)


def test_registry_has_one_row_per_table_and_layer_with_trace_paths(mini_repo):
    cfg = load_config(mini_repo(groups={"g1": ["TBL_A"]}), "sandbox")
    rows = registry_rows(cfg, git_commit="abc")

    assert [(r["layer"], r["table_fqn"]) for r in rows] == [
        ("bronze", "bronze_sandbox.testsrc.tbl_a"),
        ("silver", "silver_sandbox.testsrc.tbl_a"),
    ]
    r = rows[1]
    assert r["pipeline_name"] == "ingest-testsrc-g1-sandbox"
    assert r["config_path"] == "sources/testsrc/groups/g1/tables/tbl_a/table.yml"
    assert r["contract_path"] == "sources/testsrc/groups/g1/tables/tbl_a/contract.odcs.yaml"
    assert r["cadence_mode"] == "file_arrival" and r["max_gap_minutes"] == 1440
    assert r["scd_type"] == 1 and rows[0]["scd_type"] is None


def test_desired_tags_and_minimal_alter_statements(mini_repo):
    cfg = load_config(mini_repo(env_overrides={"governance": {"tag_prefix": "p_"}}), "sandbox")
    desired = desired_table_tags(cfg)
    fqn = "silver_sandbox.testsrc.tbl_a"

    assert desired[fqn] == {
        "p_ingest_pipeline": "ingest-testsrc-g1-sandbox",
        "p_ingest_source": "testsrc",
        "p_ingest_layer": "silver",
    }
    current = {fqn: dict(desired[fqn]), "bronze_sandbox.testsrc.tbl_a": {"p_ingest_pipeline": "wrong"}}
    statements = tag_statements({k: desired[k] for k in current}, current, existing=set(current))

    assert statements == [
        "ALTER TABLE `bronze_sandbox`.`testsrc`.`tbl_a` SET TAGS ('p_ingest_layer' = 'bronze', "
        "'p_ingest_pipeline' = 'ingest-testsrc-g1-sandbox', 'p_ingest_source' = 'testsrc')"
    ]


def test_tags_are_not_applied_to_tables_that_do_not_exist_yet(mini_repo):
    cfg = load_config(mini_repo(), "sandbox")
    assert tag_statements(desired_table_tags(cfg), {}, existing=set()) == []


def test_undeclared_tables_are_the_difference():
    existing = {"b.s.declared", "b.s.stray", "b.s.__internal"}
    assert undeclared_tables(existing, {"b.s.declared"}) == ["b.s.stray"]


def test_event_log_union_view_and_empty_placeholder():
    sql = event_log_view_sql("b.ops.v_event_log", ["b.ops.event_log_a", "b.ops.event_log_b"])
    assert sql.startswith("CREATE OR REPLACE VIEW b.ops.v_event_log AS")
    assert sql.count("UNION ALL") == 1 and "b.ops.event_log_b" in sql
    empty = event_log_view_sql("b.ops.v_event_log", [])
    assert "WHERE false" in empty and "details" in empty


def test_governed_tag_values_cover_every_pipeline_source_and_layer(mini_repo):
    cfg = load_config(mini_repo(groups={"g1": ["TBL_A"], "g2": ["TBL_B"]}), "sandbox")
    assert governed_tag_values(cfg) == {
        "ingest_layer": ["bronze", "silver"],
        "ingest_pipeline": ["ingest-testsrc-g1-sandbox", "ingest-testsrc-g2-sandbox"],
        "ingest_source": ["testsrc"],
    }


def test_trace_shows_the_pipeline_config_tag_as_deployed(mini_repo, capsys):
    """`make trace` must print the same ingest_config value the pipeline is tagged with: the group folder."""
    from ingestion_framework.adapters import get_adapter
    from ingestion_framework.governance.tags import pipeline_tags
    from ingestion_framework.trace import trace

    root = mini_repo()
    cfg = load_config(root, "sandbox")
    assert trace(root, "sandbox", table="tbl_a", pipeline=None, cli=None) == 0
    group = cfg.group("testsrc", "g1")
    deployed = pipeline_tags(cfg, group, get_adapter("qlik_cdc_files"), git_commit="-")[cfg.tag_key("ingest_config")]

    assert deployed == "sources/testsrc/groups/g1/"
    config_lines = [line for line in capsys.readouterr().out.splitlines() if "ingest_config" in line]
    assert config_lines and all(line.rstrip().endswith(f"-> {deployed}") for line in config_lines)
