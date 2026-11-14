"""SANDBOX ONLY: write synthetic Qlik Replicate files to the landing zone and verify silver.

Batches (job parameter ``batch``; ``table`` limits any batch to one table):
  full_load  one LOAD file per table in <TABLE>/ (+ .dfm), expected state reset to it
  changes    one change file per table in <TABLE>__ct/ with I/U/B/D rows (+ .dfm)
  reload     a Qlik table reload (needs ``table``): changes Qlik never delivered are only in a new snapshot,
             rewritten as LOAD00000001 (same name: the worst case). Prints the SINCE for ``make reload``
  verify     compare every silver table with the expected state; fail on any difference
  reset      delete the synthetic landing files and expected-state tables

Files are written the way Qlik does: one named file per batch, plus a .dfm metadata file the
pipeline must ignore. Expected state lives in <ops>.seed_expected_<source>_<table>.
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import UTC, datetime
from typing import Any

from ingestion_framework.adapters.qlik_cdc_files import builders as b
from ingestion_framework.config import ResolvedConfig, TableConfig
from ingestion_framework.synthetic.qlik import ChangeMix, apply_changes, generate_changes, generate_full_load

ROWS_PER_TABLE = 200
MIX = ChangeMix(inserts=10, updates=15, deletes=5)
LOST = ChangeMix(inserts=5, updates=5, deletes=5)  # what a reload must repair: never sent as change files
VIOLATION_RATE = 0.02  # a few rows outside contract `examples`, so warn-expectation metrics are visible


def _dbutils(spark: Any) -> Any:
    try:
        from databricks.sdk.runtime import dbutils

        return dbutils
    except ImportError:  # classic compute
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)


def _tables(cfg: ResolvedConfig, only: str = "") -> list[tuple[str, TableConfig]]:
    out: list[tuple[str, TableConfig]] = []
    for g in cfg.groups_in_scope():
        if cfg.source(g.source).type == "qlik_cdc_files":
            out.extend((g.landing_folder, t) for t in g.tables if not only or only.lower() in {t.name, t.table.lower()})
    if only and not out:
        raise ValueError(f"table '{only}' is not an in-scope table of this environment")
    return out


def _expected_table(cfg: ResolvedConfig, t: TableConfig) -> str:
    return f"{cfg.ops_schema_fqn}.seed_expected_{t.source}_{t.name}"


def _dfm(t: TableConfig, file_name: str, location: str, count: int, qlik: dict[str, Any], ct: bool) -> str:
    columns = [
        {
            "ordinal": i + 1,
            "name": c.name,
            "type": b.spark_type(c).upper(),
            "primaryKeyPos": c.primary_key_position or 0,
        }
        for i, c in enumerate(t.contract.columns)
    ]
    if ct:
        base = len(columns)
        columns += [
            {"ordinal": base + i + 1, "name": h, "type": ty.upper(), "primaryKeyPos": 0}
            for i, (h, ty) in enumerate(qlik["header_columns"].items())
        ]
    return json.dumps(
        {
            "dfmVersion": "1.1",
            "taskInfo": {"name": f"synthetic-{t.group}", "sourceEndpoint": t.source},
            "fileInfo": {
                "name": file_name,
                "extension": qlik.get("file_format", "parquet"),
                "location": location,
                "recordCount": count,
            },
            "dataInfo": {"sourceSchema": "dbo", "sourceTable": t.table, "columns": columns},
        },
        indent=2,
    )


def _write_file(
    spark: Any, landing: str, rows: list[dict[str, Any]], ddl: str, folder: str, name: str, dfm: str
) -> None:
    """Write one Qlik-style named file. Staging sits outside every group folder, so it never
    fires a group's file-arrival trigger or gets picked up by Auto Loader."""
    dbutils = _dbutils(spark)
    staging = f"{landing}/_seed_staging/{uuid.uuid4().hex}"
    spark.createDataFrame(rows, schema=ddl).coalesce(1).write.mode("overwrite").parquet(staging)
    part = next(f.path for f in dbutils.fs.ls(staging) if f.name.endswith(".parquet"))
    target = f"{folder}/{name}.parquet"
    dbutils.fs.rm(target)  # a rewrite replaces the file, as Qlik's drop-and-create does (no-op when absent)
    dbutils.fs.mv(part, target)
    dbutils.fs.put(f"{folder}/{name}.dfm", dfm, overwrite=True)
    dbutils.fs.rm(staging, recurse=True)


def _save_expected(spark: Any, cfg: ResolvedConfig, t: TableConfig, state: dict[Any, dict[str, Any]]) -> None:
    ddl = b.reader_schema_ddl(t.contract.columns, {}, include_headers=False)
    df = spark.createDataFrame(list(state.values()) or [], schema=ddl)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(_expected_table(cfg, t))


def _load_expected(spark: Any, cfg: ResolvedConfig, t: TableConfig) -> dict[Any, dict[str, Any]]:
    keys = t.contract.keys
    return {tuple(r[k] for k in keys): r.asDict() for r in spark.table(_expected_table(cfg, t)).collect()}


def run(spark: Any, cfg: ResolvedConfig, batch: str, *, table: str = "", seed: int | None = None) -> dict[str, Any]:
    if not cfg.env.seed_enabled:
        raise RuntimeError(f"seed is disabled for env '{cfg.env.name}' (sandbox only)")
    if batch == "reload" and not table:
        raise ValueError("batch 'reload' needs a table (a Qlik reload is per table)")
    now = datetime.now(UTC)
    rng = random.Random(seed if seed is not None else int(now.timestamp()))
    summary: dict[str, Any] = {"batch": batch, "tables": {}}
    for folder, t in _tables(cfg, table):
        qlik = dict(cfg.source(t.source).settings["qlik"])
        landing = cfg.landing_root(t.source)
        cols = t.contract.columns
        if batch == "full_load":
            rows = generate_full_load(cols, ROWS_PER_TABLE, rng, violation_rate=VIOLATION_RATE)
            target = b.full_load_path(landing, folder, t.table)
            _write_file(
                spark,
                landing,
                rows,
                b.reader_schema_ddl(cols, qlik, include_headers=False),
                target,
                "LOAD00000001",
                _dfm(t, "LOAD00000001", target, len(rows), qlik, ct=False),
            )
            _save_expected(spark, cfg, t, apply_changes(rows, [], t.contract.keys))
            summary["tables"][t.name] = {"full_load_rows": len(rows)}
        elif batch == "changes":
            state = _load_expected(spark, cfg, t)
            rows = generate_changes(cols, t.contract.keys, state, MIX, now, rng, violation_rate=VIOLATION_RATE)
            target = b.changes_path(landing, folder, t.table, str(qlik["ct_suffix"]))
            name = now.strftime("%Y%m%d-%H%M%S%f")[:-3]
            _write_file(
                spark,
                landing,
                rows,
                b.reader_schema_ddl(cols, qlik, include_headers=True),
                target,
                name,
                _dfm(t, name, target, len(rows), qlik, ct=True),
            )
            _save_expected(spark, cfg, t, state)
            summary["tables"][t.name] = {"change_rows": len(rows), "expected_rows": len(state)}
        elif batch == "reload":
            state = _load_expected(spark, cfg, t)
            since = now.strftime("%Y%m%d%H%M%S")  # the reload start: the new snapshot is written after it
            generate_changes(cols, t.contract.keys, state, LOST, now, rng, violation_rate=VIOLATION_RATE)
            rows = list(state.values())
            target = b.full_load_path(landing, folder, t.table)
            _write_file(
                spark,
                landing,
                rows,
                b.reader_schema_ddl(cols, qlik, include_headers=False),
                target,
                "LOAD00000001",
                _dfm(t, "LOAD00000001", target, len(rows), qlik, ct=False),
            )
            _save_expected(spark, cfg, t, state)
            summary["tables"][t.name] = {
                "snapshot_rows": len(rows),
                "lost_changes_repaired_by_snapshot": f"{LOST.inserts} I / {LOST.updates} U / {LOST.deletes} D",
                "next": f"make reload {cfg.env.name} TABLE={t.name} SINCE={since} PROFILE=<p>",
            }
        elif batch == "verify":
            summary["tables"][t.name] = _verify(spark, cfg, t)
        elif batch == "reset":
            _dbutils(spark).fs.rm(b.full_load_path(landing, folder, t.table), recurse=True)
            _dbutils(spark).fs.rm(b.changes_path(landing, folder, t.table, str(qlik["ct_suffix"])), recurse=True)
            spark.sql(f"DROP TABLE IF EXISTS {_expected_table(cfg, t)}")
            summary["tables"][t.name] = "reset"
        else:
            raise ValueError(f"unknown batch '{batch}' (full_load | changes | reload | verify | reset)")
    if batch == "verify":
        bad = {k: v for k, v in summary["tables"].items() if v["missing"] or v["unexpected"]}
        summary["ok"] = not bad
        if bad:
            raise AssertionError(f"silver differs from expected state: {json.dumps(bad, indent=2)}")
    return summary


def _verify(spark: Any, cfg: ResolvedConfig, t: TableConfig) -> dict[str, int]:
    silver = cfg.table_fqn("silver", t.source, t.name)
    cols = ", ".join(f"`{c.name}`" for c in t.contract.columns)
    current = f"SELECT {cols} FROM {silver}" + (" WHERE __END_AT IS NULL" if t.scd_type == 2 else "")
    expected = f"SELECT {cols} FROM {_expected_table(cfg, t)}"
    missing = spark.sql(f"{expected} EXCEPT ALL {current}").count()
    unexpected = spark.sql(f"{current} EXCEPT ALL {expected}").count()
    return {"silver_rows": spark.sql(current).count(), "missing": missing, "unexpected": unexpected}
