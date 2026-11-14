from __future__ import annotations

import json

import pytest
import yaml

from ingestion_framework.config import load_config
from ingestion_framework.contracts.validate import profile_problems, schema_problems
from ingestion_framework.scaffold import ScaffoldError, scaffold_from_csv, scaffold_from_dfm, spark_type_for

CSV = """table,column,position,data_type,nullable,primary_key_position,description
CMC_CLCL_CLAIM,CLCL_ID,1,char(12),N,1,Claim identifier
CMC_CLCL_CLAIM,MEME_CK,2,int,N,,Member key
CMC_CLCL_CLAIM,CLCL_TOT_CHG,3,"money",Y,,Total charge
CMC_CDML_CL_LINE,CLCL_ID,1,char(12),N,1,
CMC_CDML_CL_LINE,CDML_SEQ_NO,2,smallint,N,2,
CMC_CDML_CL_LINE,CDML_FROM_DT,3,datetime,Y,,
"""

DFM = {
    "dfmVersion": "1.1",
    "dataInfo": {
        "sourceSchema": "dbo",
        "sourceTable": "CMC_PRPR_PROV",
        "columns": [
            {"ordinal": 1, "name": "PRPR_ID", "type": "STRING", "length": 12, "primaryKeyPos": 1},
            {"ordinal": 2, "name": "PRPR_NAME", "type": "WSTRING", "length": 55, "primaryKeyPos": 0},
            {"ordinal": 3, "name": "PRPR_TERM_DT", "type": "DATETIME", "primaryKeyPos": 0},
            {
                "ordinal": 4,
                "name": "PRPR_RATE",
                "type": "NUMERIC",
                "precision": 9,
                "scale": 4,
                "primaryKeyPos": 0,
            },
        ],
    },
}


@pytest.fixture
def repo(mini_repo):
    return mini_repo(groups={"g1": ["TBL_A"]})


def test_csv_scaffold_writes_table_config_and_valid_contract_pairs(repo, tmp_path):
    src = tmp_path / "dict.csv"
    src.write_text(CSV)
    result = scaffold_from_csv(repo, source="testsrc", group="g1", path=src, owner="claims-data", status="active")

    assert sorted(result.created) == ["cmc_cdml_cl_line", "cmc_clcl_claim"]
    gdir = repo / "sources" / "testsrc" / "groups" / "g1"
    assert yaml.safe_load((gdir / "cmc_clcl_claim.yml").read_text()) == {
        "table": "CMC_CLCL_CLAIM",
        "enabled": True,
    }

    doc = yaml.safe_load((gdir / "cmc_cdml_cl_line.odcs.yaml").read_text())
    assert schema_problems(doc) == [] and profile_problems(doc) == []
    props = {p["name"]: p for p in doc["schema"][0]["properties"]}
    assert props["CDML_SEQ_NO"]["primaryKeyPosition"] == 2
    assert props["CDML_FROM_DT"]["physicalType"] == "timestamp"
    assert doc["team"]["members"][0]["username"] == "claims-data"

    cfg = load_config(repo, "sandbox")
    lines = next(t for t in cfg.group("testsrc", "g1").tables if t.name == "cmc_cdml_cl_line")
    assert lines.contract.keys == ("CLCL_ID", "CDML_SEQ_NO")


def test_existing_files_are_never_overwritten(repo, tmp_path):
    src = tmp_path / "dict.csv"
    src.write_text(CSV)
    scaffold_from_csv(repo, source="testsrc", group="g1", path=src)
    second = scaffold_from_csv(repo, source="testsrc", group="g1", path=src)

    assert second.created == [] and sorted(second.skipped) == ["cmc_cdml_cl_line", "cmc_clcl_claim"]


def test_dfm_scaffold_maps_qlik_types_and_keys(repo, tmp_path):
    dfm = tmp_path / "CMC_PRPR_PROV.dfm"
    dfm.write_text(json.dumps(DFM))
    result = scaffold_from_dfm(repo, source="testsrc", group="g1", paths=[dfm])

    assert result.created == ["cmc_prpr_prov"]
    doc = yaml.safe_load((repo / "sources/testsrc/groups/g1/cmc_prpr_prov.odcs.yaml").read_text())
    props = {p["name"]: p for p in doc["schema"][0]["properties"]}
    assert props["PRPR_ID"]["primaryKey"] is True
    assert props["PRPR_RATE"]["physicalType"] == "decimal(9,4)"
    assert props["PRPR_TERM_DT"]["logicalType"] == "timestamp"
    assert "primaryKey" not in props["PRPR_NAME"]


@pytest.mark.parametrize(
    ("source_type", "expected"),
    [
        ("int", "int"),
        ("bigint", "bigint"),
        ("varchar(40)", "string"),
        ("decimal(18,2)", "decimal(18,2)"),
        ("money", "decimal(19,4)"),
        ("datetime", "timestamp"),
        ("date", "date"),
        ("bit", "boolean"),
        ("float", "double"),
    ],
)
def test_source_types_map_to_spark_types(source_type, expected):
    assert spark_type_for(source_type) == expected


def test_unknown_type_and_missing_group_fail_clearly(repo, tmp_path):
    with pytest.raises(ScaffoldError, match="geography"):
        spark_type_for("geography")
    src = tmp_path / "dict.csv"
    src.write_text(CSV)
    with pytest.raises(ScaffoldError, match="group 'nope' does not exist"):
        scaffold_from_csv(repo, source="testsrc", group="nope", path=src)
