"""Scaffold per-table config + ODCS contract pairs from a data dictionary or Qlik .dfm files.

This is how ~2,700 tables get onboarded without hand-writing YAML:

    make scaffold SOURCE=facets GROUP=claims FROM=csv PATH=exports/claims_dictionary.csv
    make scaffold SOURCE=facets GROUP=claims FROM=dfm PATH=landing/claims/CMC_CLCL_CLAIM__ct

Existing files are never overwritten (review the diff, then hand-add quality rules).

CSV columns: table, column, position, data_type, nullable (Y/N), primary_key_position, description
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from ingestion_framework.config import layout
from ingestion_framework.contracts.validate import ODCS_VERSION


class ScaffoldError(ValueError):
    pass


@dataclass
class ScaffoldResult:
    created: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SourceColumn:
    name: str
    position: int
    spark_type: str
    nullable: bool
    pk_position: int | None
    description: str | None = None


# ---- type mapping ---------------------------------------------------------------------
_DECIMAL = re.compile(r"^(?:decimal|numeric|number)\s*\((\d+)\s*,\s*(\d+)\)$")
_SIMPLE = {
    # relational source types (Facets runs on SQL Server / Sybase / Oracle)
    "tinyint": "int",
    "smallint": "int",
    "int": "int",
    "integer": "int",
    "bigint": "bigint",
    "bit": "boolean",
    "boolean": "boolean",
    "float": "double",
    "real": "float",
    "double": "double",
    "money": "decimal(19,4)",
    "smallmoney": "decimal(10,4)",
    "date": "date",
    "datetime": "timestamp",
    "datetime2": "timestamp",
    "smalldatetime": "timestamp",
    "timestamp": "timestamp",
    "time": "string",
    "text": "string",
    "ntext": "string",
    "uniqueidentifier": "string",
    "binary": "binary",
    "varbinary": "binary",
    "image": "binary",
    # Qlik Replicate (.dfm) types
    "int1": "int",
    "int2": "int",
    "int4": "int",
    "int8": "bigint",
    "uint1": "int",
    "uint2": "int",
    "uint4": "bigint",
    "uint8": "decimal(20,0)",
    "real4": "float",
    "real8": "double",
    "string": "string",
    "wstring": "string",
    "clob": "string",
    "nclob": "string",
    "bytes": "binary",
    "blob": "binary",
}
_STRINGY = re.compile(r"^(?:n?var)?n?char(?:acter)?(?:\s*\(\s*(?:\d+|max)\s*\))?$|^varchar2.*$|^nvarchar2.*$")
_LOGICAL = {
    "int": "integer",
    "bigint": "integer",
    "float": "number",
    "double": "number",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "string": "string",
    "binary": "string",
}


def spark_type_for(source_type: str, *, precision: int | None = None, scale: int | None = None) -> str:
    t = source_type.strip().lower()
    if t in {"numeric", "decimal", "number"} and precision:
        return f"decimal({precision},{scale or 0})"
    if m := _DECIMAL.match(t):
        return f"decimal({m.group(1)},{m.group(2)})"
    if t in _SIMPLE:
        return _SIMPLE[t]
    if _STRINGY.match(t):
        return "string"
    raise ScaffoldError(f"no Spark type mapping for source type '{source_type}' — add it to scaffold._SIMPLE")


def logical_type_for(spark_type: str) -> str:
    return "number" if spark_type.startswith("decimal") else _LOGICAL.get(spark_type, "string")


# ---- readers --------------------------------------------------------------------------
def read_csv_dictionary(path: Path) -> dict[str, list[SourceColumn]]:
    tables: dict[str, list[SourceColumn]] = defaultdict(list)
    with path.open(newline="") as fh:
        for row in csv.DictReader(fh):
            pk = (row.get("primary_key_position") or "").strip()
            tables[row["table"].strip()].append(
                SourceColumn(
                    name=row["column"].strip(),
                    position=int(row["position"]),
                    spark_type=spark_type_for(row["data_type"]),
                    nullable=(row.get("nullable") or "Y").strip().upper() != "N",
                    pk_position=int(pk) if pk else None,
                    description=(row.get("description") or "").strip() or None,
                )
            )
    return dict(tables)


def read_dfm(path: Path) -> tuple[str, list[SourceColumn]]:
    doc = json.loads(path.read_text())
    info = doc.get("dataInfo") or {}
    table = info.get("sourceTable") or info.get("targetTable")
    if not table:
        raise ScaffoldError(f"{path}: dataInfo.sourceTable missing — is this a Qlik .dfm file?")
    columns: list[SourceColumn] = []
    for col in info.get("columns") or []:
        pk = int(col.get("primaryKeyPos") or 0)
        columns.append(
            SourceColumn(
                name=str(col["name"]),
                position=int(col.get("ordinal", len(columns) + 1)),
                spark_type=spark_type_for(str(col["type"]), precision=col.get("precision"), scale=col.get("scale")),
                nullable=pk == 0,
                pk_position=pk or None,
            )
        )
    return str(table), columns


# ---- writers --------------------------------------------------------------------------
def contract_for(
    source: str,
    table: str,
    columns: Iterable[SourceColumn],
    *,
    domain: str,
    owner: str,
    status: str,
    classification: str,
    description: str | None = None,
) -> dict[str, Any]:
    props: list[dict[str, Any]] = []
    for col in sorted(columns, key=lambda c: c.position):
        prop: dict[str, Any] = {
            "name": col.name,
            "logicalType": logical_type_for(col.spark_type),
            "physicalType": col.spark_type,
        }
        if col.pk_position:
            prop["primaryKey"] = True
            prop["primaryKeyPosition"] = col.pk_position
        if not col.nullable or col.pk_position:
            prop["required"] = True
        if col.description:
            prop["description"] = col.description
        props.append(prop)
    return {
        "apiVersion": ODCS_VERSION,
        "kind": "DataContract",
        "id": f"urn:ingest:{source}:{table.lower()}",
        "name": f"{source}.{table.lower()}",
        "version": "1.0.0",
        "status": status,
        "domain": domain,
        "dataProduct": source,
        "description": {"purpose": description or f"Operational replica of {source} {table}."},
        "team": {"name": owner, "members": [{"username": owner, "role": "owner"}]},
        "schema": [
            {
                "name": table.lower(),
                "physicalName": table,
                "logicalType": "object",
                "physicalType": "table",
                "description": description or f"{source} {table}",
                "properties": props,
            }
        ],
        "slaProperties": [{"property": "latency", "value": 60, "unit": "m", "description": "Change visible in silver"}],
        "customProperties": [{"property": "dataClassification", "value": classification}],
    }


def _group_dir(root: Path, source: str, group: str) -> Path:
    gdir = layout.group_dir(root, source, group)
    if not (gdir / layout.GROUP_FILE).exists():
        raise ScaffoldError(
            f"group '{group}' does not exist under sources/{source}/{layout.GROUPS_DIR} "
            f"(create {layout.GROUP_FILE} first)"
        )
    return gdir


def _write_pairs(
    root: Path,
    source: str,
    group: str,
    tables: Mapping[str, list[SourceColumn]],
    *,
    owner: str,
    status: str,
) -> ScaffoldResult:
    gdir = _group_dir(root, source, group)
    group_doc = yaml.safe_load((gdir / layout.GROUP_FILE).read_text()) or {}
    source_doc = yaml.safe_load((root / "sources" / source / layout.SOURCE_FILE).read_text()) or {}
    classification = str((source_doc.get("defaults") or {}).get("classification", "internal"))
    result = ScaffoldResult()
    for table, columns in sorted(tables.items()):
        stem = table.lower()
        tdir = layout.table_dir(gdir, stem)
        cfg_path, contract_path = tdir / layout.TABLE_FILE, tdir / layout.CONTRACT_FILE
        if cfg_path.exists() or contract_path.exists():
            result.skipped.append(stem)
            continue
        if not columns:
            raise ScaffoldError(f"{table}: no columns")
        tdir.mkdir(parents=True, exist_ok=True)
        cfg_path.write_text(yaml.safe_dump({"table": table, "enabled": True}, sort_keys=False))
        doc = contract_for(
            source,
            table,
            columns,
            domain=str(group_doc.get("domain", group)),
            owner=owner,
            status=status,
            classification=classification,
        )
        contract_path.write_text(yaml.safe_dump(doc, sort_keys=False, width=110))
        result.created.append(stem)
    return result


def scaffold_from_csv(
    root: Path, *, source: str, group: str, path: Path, owner: str = "data-platform", status: str = "draft"
) -> ScaffoldResult:
    return _write_pairs(root, source, group, read_csv_dictionary(path), owner=owner, status=status)


def scaffold_from_dfm(
    root: Path,
    *,
    source: str,
    group: str,
    paths: Iterable[Path],
    owner: str = "data-platform",
    status: str = "draft",
) -> ScaffoldResult:
    tables: dict[str, list[SourceColumn]] = {}
    for p in paths:
        files = sorted(p.rglob("*.dfm")) if p.is_dir() else [p]
        for f in files:
            table, cols = read_dfm(f)
            tables.setdefault(table, cols)  # one .dfm per table is enough
    return _write_pairs(root, source, group, tables, owner=owner, status=status)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path.cwd())
    ap.add_argument("--source", required=True)
    ap.add_argument("--group", required=True)
    ap.add_argument("--from", dest="kind", choices=["csv", "dfm"], required=True)
    ap.add_argument("--path", type=Path, required=True, nargs="+")
    ap.add_argument("--owner", default="data-platform")
    ap.add_argument("--status", default="draft", choices=["draft", "proposed", "active"])
    args = ap.parse_args(argv)
    try:
        if args.kind == "csv":
            result = ScaffoldResult()
            for p in args.path:
                r = scaffold_from_csv(
                    args.root,
                    source=args.source,
                    group=args.group,
                    path=p,
                    owner=args.owner,
                    status=args.status,
                )
                result.created += r.created
                result.skipped += r.skipped
        else:
            result = scaffold_from_dfm(
                args.root,
                source=args.source,
                group=args.group,
                paths=args.path,
                owner=args.owner,
                status=args.status,
            )
    except ScaffoldError as exc:
        print(f"scaffold: {exc}", file=sys.stderr)
        return 2
    print(f"created {len(result.created)}: {', '.join(result.created) or '-'}")
    print(f"skipped {len(result.skipped)} (already declared): {', '.join(result.skipped) or '-'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
