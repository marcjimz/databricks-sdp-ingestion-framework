from __future__ import annotations

import yaml

from ingestion_framework.lint import lint_repo


def _messages(root):
    return [f"{p.path}: {p.message}" for p in lint_repo(root) if p.severity == "error"]


def test_clean_repo_has_no_errors(mini_repo):
    assert _messages(mini_repo()) == []


def test_table_without_contract_and_orphan_contract_are_errors(mini_repo):
    root = mini_repo(groups={"g1": ["TBL_A", "TBL_B"]})
    gdir = root / "sources/testsrc/groups/g1"
    (gdir / "tbl_a.odcs.yaml").unlink()
    (gdir / "tbl_b.yml").unlink()
    msgs = " ".join(_messages(root))

    assert "tbl_a.yml" in msgs and "no data contract" in msgs
    assert "tbl_b.odcs.yaml" in msgs and "orphan contract" in msgs


def test_invalid_odcs_and_house_profile_are_errors(mini_repo):
    root = mini_repo()
    path = root / "sources/testsrc/groups/g1/tbl_a.odcs.yaml"
    doc = yaml.safe_load(path.read_text())
    doc["kind"] = "Nope"
    doc.pop("slaProperties")
    path.write_text(yaml.safe_dump(doc))
    msgs = " ".join(_messages(root))

    assert "kind" in msgs and "slaProperties" in msgs


def test_cdc_table_without_primary_key_is_an_error_unless_append_only(mini_repo):
    cols = [{"name": "V", "logicalType": "string", "physicalType": "string"}]
    root = mini_repo(columns=cols, groups={"g1": ["TBL_A", "TBL_B"]}, table_overrides={"TBL_B": {"append_only": True}})
    msgs = _messages(root)

    assert any("tbl_a" in m and "primary key" in m for m in msgs)
    assert not any("tbl_b" in m and "primary key" in m for m in msgs)


def test_drop_actions_and_unsupported_rules_are_errors(mini_repo):
    root = mini_repo(
        quality=[
            {
                "type": "custom",
                "engine": "databricks-sdp",
                "implementation": "ID > 0",
                "name": "x",
                "customProperties": [{"property": "action", "value": "drop"}],
            },
            {"type": "library", "metric": "rowCount", "mustBeGreaterThan": 0},
        ]
    )
    msgs = " ".join(_messages(root))
    assert "'drop' not allowed" in msgs and "library" in msgs


def test_physical_name_must_match_table_config(mini_repo):
    root = mini_repo()
    path = root / "sources/testsrc/groups/g1/tbl_a.odcs.yaml"
    path.write_text(path.read_text().replace("physicalName: TBL_A", "physicalName: SOMETHING_ELSE"))
    assert any("physicalName" in m for m in _messages(root))


def test_group_cap_duplicates_scope_and_cadence_rules(mini_repo):
    root = mini_repo(
        groups={"g1": ["TBL_A"], "g2": ["TBL_A"]},
        env_overrides={
            "scope": {"groups": ["testsrc/missing"]},
            "cadence": {"default": "on_arrival", "overrides": {"testsrc/g1": "no_such_profile"}},
        },
    )
    fw = root / "conf/framework.yml"
    doc = yaml.safe_load(fw.read_text())
    doc["limits"]["max_tables_per_group"] = 0
    doc["cadence_profiles"]["monthly"] = {
        "mode": "cron",
        "quartz_cron_expression": "0 0 6 1 * ?",
        "max_gap_days": 31,
    }
    fw.write_text(yaml.safe_dump(doc))
    env = root / "conf/environments/sandbox.yml"
    env_doc = yaml.safe_load(env.read_text())
    env_doc["cadence"]["overrides"]["testsrc/g2"] = "monthly"
    env.write_text(yaml.safe_dump(env_doc))
    msgs = " ".join(_messages(root))

    assert "duplicate table name" in msgs
    assert "exceeds max_tables_per_group" in msgs
    assert "scope.groups" in msgs and "testsrc/missing" in msgs
    assert "no_such_profile" in msgs
    assert "31" in msgs and "directory listing" in msgs


def test_unknown_source_type_is_an_error(mini_repo):
    root = mini_repo()
    src = root / "sources/testsrc/source.yml"
    src.write_text(src.read_text().replace("type: qlik_cdc_files", "type: carrier_pigeon"))
    assert any("carrier_pigeon" in m for m in _messages(root))


def test_local_override_files_must_not_hold_secrets(mini_repo):
    root = mini_repo()
    (root / "conf/environments/sandbox.local.yml").write_text(yaml.safe_dump({"deployer": {"client_secret": "x"}}))
    assert any("client_secret" in m and "deployed" in m for m in _messages(root))
