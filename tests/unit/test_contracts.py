from __future__ import annotations

import copy
from pathlib import Path

import pytest

from ingestion_framework.config import ConfigError
from ingestion_framework.contracts import parse_contract, unsupported_rules
from ingestion_framework.contracts.validate import profile_problems, schema_problems
from tests.conftest import DEFAULT_COLUMNS, contract_doc

POLICY = {"allowed_actions": ["warn", "fail"], "default_action": "warn", "sdp_engine": "databricks-sdp"}


def _rule(impl: str, name: str, action: str | None = None) -> dict:
    rule = {"type": "custom", "engine": "databricks-sdp", "implementation": impl, "name": name}
    if action:
        rule["customProperties"] = [{"property": "action", "value": action}]
    return rule


def test_required_and_primary_key_columns_become_not_null_expectations():
    c = parse_contract(contract_doc("T", DEFAULT_COLUMNS), Path("t.odcs.yaml"), POLICY)

    assert c.keys == ("ID",)
    assert [(e.name, e.condition, e.action) for e in c.expectations] == [("id_not_null", "`ID` IS NOT NULL", "warn")]


def test_custom_sdp_rules_map_to_expectations_with_actions():
    cols = copy.deepcopy(DEFAULT_COLUMNS)
    cols[2]["quality"] = [_rule("${column} >= 0", "amount_non_negative", "fail")]
    doc = contract_doc("T", cols, quality=[_rule("NAME <> ''", "name_not_blank")])
    c = parse_contract(doc, Path("t.odcs.yaml"), POLICY)

    by_name = {e.name: e for e in c.expectations}
    assert by_name["amount_non_negative"].condition == "`AMOUNT` >= 0"
    assert by_name["amount_non_negative"].action == "fail"
    assert by_name["name_not_blank"].action == "warn"


def test_composite_keys_follow_primary_key_position():
    cols = [
        {"name": "B", "logicalType": "integer", "primaryKey": True, "primaryKeyPosition": 2},
        {"name": "A", "logicalType": "string", "primaryKey": True, "primaryKeyPosition": 1},
        {"name": "V", "logicalType": "string"},
    ]
    c = parse_contract(contract_doc("T", cols), Path("t.odcs.yaml"), POLICY)
    assert c.keys == ("A", "B")


def test_one_schema_object_per_table_contract():
    doc = contract_doc("T", DEFAULT_COLUMNS)
    doc["schema"].append(copy.deepcopy(doc["schema"][0]))
    with pytest.raises(ConfigError, match="exactly one schema object"):
        parse_contract(doc, Path("t.odcs.yaml"), POLICY)


def test_unsupported_rules_and_drop_action_are_reported():
    doc = contract_doc(
        "T",
        DEFAULT_COLUMNS,
        quality=[
            {"type": "library", "metric": "rowCount", "mustBeGreaterThan": 0},
            {"type": "custom", "engine": "soda", "implementation": "x"},
            _rule("ID > 0", "id_positive", "drop"),
            {"type": "text", "description": "documentation only"},
        ],
    )
    problems = unsupported_rules(doc, POLICY)

    assert len(problems) == 3
    assert any("rowCount" in p or "library" in p for p in problems)
    assert any("soda" in p for p in problems)
    assert any("'drop' not allowed" in p for p in problems)


def test_valid_contract_passes_odcs_schema_and_house_profile():
    doc = contract_doc("T", DEFAULT_COLUMNS)
    assert schema_problems(doc) == []
    assert profile_problems(doc) == []


def test_schema_violations_are_reported_with_paths():
    doc = contract_doc("T", DEFAULT_COLUMNS)
    doc["kind"] = "NotAContract"
    doc["schema"][0]["properties"][0]["logicalType"] = "uuid-ish"
    problems = schema_problems(doc)

    assert any("kind" in p for p in problems)
    assert any("logicalType" in p for p in problems)


def test_house_profile_requires_status_team_sla_and_classification():
    doc = contract_doc("T", DEFAULT_COLUMNS)
    for key in ("status", "team", "slaProperties", "customProperties"):
        doc.pop(key)
    problems = " ".join(profile_problems(doc))

    for needle in ("status", "team", "slaProperties", "dataClassification"):
        assert needle in problems
