"""Pure SQL-expression builders for Qlik Replicate change files.

Everything here returns strings (SQL expressions, DDL, option maps) so it is unit-testable
without Spark; :mod:`.runtime` feeds them to Auto Loader and SDP.

Qlik layout and semantics this encodes:
  * ``<TABLE>/``     full-load files, no ``header__`` columns
  * ``<TABLE>__ct/`` change files with ``header__change_seq`` (ordered within a task) and
                     ``header__change_oper`` in I/U/D/B (B = before image, same seq as its U)
  * bronze keeps every row (append-only history); silver applies them with AUTO CDC.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from ingestion_framework.config import Column

LOAD_TYPE = "_load_type"
CDC_SEQ = "_cdc_seq"
INGEST_FILE = "_ingest_file"
FILE_MODIFIED = "_file_modification_time"
INGEST_TS = "_ingest_ts"
RESCUED = "_rescued_data"

_LOGICAL_TO_SPARK = {
    "string": "string",
    "integer": "bigint",
    "number": "double",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "time": "string",
}


def q(name: str) -> str:
    """Backtick-quote an identifier."""
    return f"`{name}`"


def spark_type(column: Column) -> str:
    if column.physical_type:
        return column.physical_type
    try:
        return _LOGICAL_TO_SPARK[column.logical_type]
    except KeyError as exc:
        raise ValueError(f"column {column.name}: no Spark type for logicalType '{column.logical_type}'") from exc


def _headers(qlik: Mapping[str, Any]) -> Mapping[str, str]:
    headers: Mapping[str, str] = qlik["header_columns"]
    return headers


def _oper(qlik: Mapping[str, Any]) -> str:
    return next(h for h in _headers(qlik) if h.endswith("change_oper"))


def _seq(qlik: Mapping[str, Any]) -> str:
    return next(h for h in _headers(qlik) if h.endswith("change_seq"))


# ---- paths -----------------------------------------------------------------------
def full_load_path(landing_root: str, group_folder: str, table: str) -> str:
    return f"{landing_root.rstrip('/')}/{group_folder}/{table}"


def changes_path(landing_root: str, group_folder: str, table: str, ct_suffix: str) -> str:
    return f"{full_load_path(landing_root, group_folder, table)}{ct_suffix}"


# ---- ordering ----------------------------------------------------------------------
FILE_TIME_FORMAT = "yyyyMMddHHmmssSSS"  # 17 chars


def cdc_seq_full_load_expr(seq_length: int) -> str:
    """Full-load rows sort before every change row (rank 0 vs 1); among snapshots, a later
    file (a Qlik reload) outranks an earlier one. Same string length as change sequences."""
    pad = seq_length - len(FILE_TIME_FORMAT)
    return f"concat('0', date_format(_metadata.file_modification_time, '{FILE_TIME_FORMAT}'), repeat('0', {pad}))"


def cdc_seq_change_expr(seq_column: str, seq_length: int) -> str:
    return f"concat('1', coalesce({q(seq_column)}, repeat('0', {seq_length})))"


# ---- reader ------------------------------------------------------------------------
def reader_schema_ddl(columns: Sequence[Column], qlik: Mapping[str, Any], *, include_headers: bool) -> str:
    """Explicit Auto Loader schema: the contract's columns (+ Qlik headers for change files).

    Supplying the schema (rather than inferring it) keeps bronze declared by the contract and
    routes undeclared columns into ``_rescued_data`` instead of silently evolving the table.
    """
    parts = [f"{q(c.name)} {spark_type(c)}" for c in columns]
    if include_headers:
        parts += [f"{q(h)} {t}" for h, t in _headers(qlik).items()]
    return ", ".join(parts)


def reader_options(
    qlik: Mapping[str, Any],
    *,
    allow_overwrites: bool = False,
    lookback_days: int | None = None,
    now: datetime | None = None,
) -> dict[str, str]:
    """Auto Loader options. ``allow_overwrites`` is for the full-load folder only: a Qlik table reload may
    rewrite ``LOAD00000001`` under the same name, and Auto Loader would otherwise skip that path as seen."""
    fmt = str(qlik.get("file_format", "parquet"))
    opts = {
        "cloudFiles.format": fmt,
        # Event-driven discovery only (no directory listing, no legacy notifications).
        "cloudFiles.useManagedFileEvents": "true",
        "cloudFiles.schemaEvolutionMode": "rescue",
        "rescuedDataColumn": RESCUED,
        "pathGlobFilter": f"*.{fmt}",
    }
    if fmt == "csv":
        opts["header"] = "true"
    if allow_overwrites:
        opts["cloudFiles.allowOverwrites"] = "true"
    opts.update({str(k): str(v) for k, v in (qlik.get("reader_options") or {}).items()})
    if lookback_days:
        cutoff = (now or datetime.now(UTC)) - timedelta(days=lookback_days)
        opts["modifiedAfter"] = cutoff.strftime("%Y-%m-%dT%H:%M:%S")
    return opts


# ---- bronze ------------------------------------------------------------------------
def _metadata_exprs(load_type: str, seq_expr: str) -> list[str]:
    return [
        f"'{load_type}' AS {q(LOAD_TYPE)}",
        f"{seq_expr} AS {q(CDC_SEQ)}",
        f"_metadata.file_path AS {q(INGEST_FILE)}",
        f"_metadata.file_modification_time AS {q(FILE_MODIFIED)}",
        f"current_timestamp() AS {q(INGEST_TS)}",
        f"{q(RESCUED)} AS {q(RESCUED)}",
    ]


def bronze_select_full_load(columns: Sequence[Column], qlik: Mapping[str, Any]) -> list[str]:
    exprs = [f"{q(c.name)} AS {q(c.name)}" for c in columns]
    for header, htype in _headers(qlik).items():
        if header == _oper(qlik):
            exprs.append(f"'{qlik['full_load_op']}' AS {q(header)}")
        else:
            exprs.append(f"CAST(NULL AS {htype}) AS {q(header)}")
    return exprs + _metadata_exprs("FULL", cdc_seq_full_load_expr(int(qlik["change_seq_length"])))


def bronze_select_changes(columns: Sequence[Column], qlik: Mapping[str, Any]) -> list[str]:
    exprs = [f"{q(c.name)} AS {q(c.name)}" for c in columns]
    exprs += [f"{q(h)} AS {q(h)}" for h in _headers(qlik)]
    seq = cdc_seq_change_expr(_seq(qlik), int(qlik["change_seq_length"]))
    return exprs + _metadata_exprs("CDC", seq)


# ---- silver ------------------------------------------------------------------------
def silver_source_filter(qlik: Mapping[str, Any], *, reload_cutoff: str | None = None) -> str:
    """Drop before-images. After a Qlik reload at ``reload_cutoff`` (yyyyMMddHHmmss), keep only the
    new snapshot (file written at/after the cutoff) and changes committed at/after it — so an old
    snapshot row deleted before the reload cannot resurface as a zombie. Qlik change sequences
    start with the commit timestamp, so a prefix comparison is exact."""
    expr = f"{q(_oper(qlik))} <> '{qlik['ops']['before_image']}'"
    if reload_cutoff:
        expr += (
            f" AND (({q(LOAD_TYPE)} = 'FULL' AND date_format({q(FILE_MODIFIED)}, 'yyyyMMddHHmmss') >= "
            f"'{reload_cutoff}') OR ({q(LOAD_TYPE)} = 'CDC' AND {q(_seq(qlik))} >= '{reload_cutoff}'))"
        )
    return expr


def silver_projection(columns: Sequence[Column], qlik: Mapping[str, Any]) -> list[str]:
    exprs = [f"CAST({q(c.name)} AS {spark_type(c)}) AS {q(c.name)}" for c in columns]
    return [*exprs, q(_oper(qlik)), q(CDC_SEQ)]


def silver_except_columns(qlik: Mapping[str, Any]) -> list[str]:
    return [_oper(qlik), CDC_SEQ]


def delete_condition(qlik: Mapping[str, Any]) -> str:
    return f"{q(_oper(qlik))} = '{qlik['ops']['delete']}'"
