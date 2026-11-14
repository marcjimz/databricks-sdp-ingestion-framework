"""Synthetic Qlik Replicate data + a reference ("oracle") implementation of the silver semantics.

Pure Python (no Spark), driven entirely by a table's contract columns. Used two ways:
  * unit tests pin the CDC semantics the pipeline must reproduce;
  * the sandbox seed job writes these rows as Qlik-shaped files and stores the oracle's expected
    state, so `make verify sandbox` can compare silver against it row by row.

Semantics encoded (and mirrored by the pipeline):
  * full-load rows sort before every change row; change rows sort by header__change_seq
  * B (before image) rows are ignored; I/U upsert; D deletes
  * replaying a change already reflected in the full load is a no-op (Qlik "cached changes")
  * after a reload, change rows older than the cutoff are ignored
"""

from __future__ import annotations

import random
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from ingestion_framework.config import Column

OPER = "header__change_oper"
SEQ = "header__change_seq"
OPERATION_NAMES = {"I": "INSERT", "U": "UPDATE", "D": "DELETE", "B": "BEFOREIMAGE"}

Row = dict[str, Any]
Key = tuple[Any, ...]


@dataclass(frozen=True)
class ChangeMix:
    inserts: int = 0
    updates: int = 0
    deletes: int = 0


def change_seq(ts: datetime, counter: int) -> str:
    """35-char, lexicographically increasing: YYYYMMDDHHMMSS + 21-digit counter (Qlik-like)."""
    return f"{ts.astimezone(UTC):%Y%m%d%H%M%S}{counter:021d}"


def _spark(c: Column) -> str:
    from ingestion_framework.adapters.qlik_cdc_files.builders import spark_type

    return spark_type(c).lower()


def _scale(spark_type: str) -> int:
    inside = spark_type[spark_type.find("(") + 1 : spark_type.find(")")] if "(" in spark_type else ""
    return int(inside.split(",")[1]) if "," in inside else 0


def _key_value(c: Column, i: int) -> Any:
    t = _spark(c)
    if t in {"int", "bigint", "smallint", "tinyint"}:
        return i + 1
    if t.startswith("decimal"):
        return Decimal(i + 1)
    return f"{c.name[:4]}{i:08d}"


def _values(columns: Sequence[Column], rng: random.Random, violation_rate: float) -> Row:
    row: Row = {}
    previous_amount: Decimal | None = None
    for c in columns:
        if c.primary_key_position is not None:
            continue
        t = _spark(c)
        if c.examples:
            if violation_rate and rng.random() < violation_rate:
                row[c.name] = "?"  # deliberately outside the contract's examples
            else:
                row[c.name] = rng.choice(c.examples)
        elif t in {"int", "bigint", "smallint", "tinyint"}:
            row[c.name] = rng.randint(1, 1_000_000)
        elif t.startswith("decimal"):
            # Non-increasing left to right (charge >= allowed >= paid) so cross-column rules hold.
            top = previous_amount if previous_amount is not None else Decimal(rng.uniform(100, 10_000))
            value = top * Decimal(rng.uniform(0.5, 1.0)) if previous_amount is not None else top
            quant = Decimal(1).scaleb(-_scale(t))
            row[c.name] = previous_amount = value.quantize(quant, rounding=ROUND_HALF_UP)
        elif t in {"double", "float"}:
            row[c.name] = round(rng.uniform(0, 1000), 4)
        elif t == "date":
            row[c.name] = date(1940, 1, 1) + timedelta(days=rng.randint(0, 30_000))
        elif t == "timestamp":
            row[c.name] = datetime(1940, 1, 1) + timedelta(seconds=rng.randint(0, 2_500_000_000))
        elif t == "boolean":
            row[c.name] = rng.random() < 0.5
        elif t == "binary":
            row[c.name] = rng.randbytes(8)
        else:
            row[c.name] = f"{c.name.lower()[:6]}_{rng.randint(0, 99_999):05d}"
    return row


def _key_of(row: Mapping[str, Any], keys: Sequence[str]) -> Key:
    return tuple(row[k] for k in keys)


def generate_full_load(
    columns: Sequence[Column], n: int, rng: random.Random, *, violation_rate: float = 0.0
) -> list[Row]:
    rows = []
    for i in range(n):
        row = {c.name: _key_value(c, i) for c in columns if c.primary_key_position is not None}
        row.update(_values(columns, rng, violation_rate))
        rows.append({c.name: row[c.name] for c in columns})
    return rows


def _header(op: str, seq: str, ts: datetime, txn: int) -> Row:
    return {
        SEQ: seq,
        OPER: op,
        "header__change_mask": None,
        "header__stream_position": f"{seq[-12:]}",
        "header__operation": OPERATION_NAMES[op],
        "header__transaction_id": f"{txn:016x}",
        "header__timestamp": ts.replace(tzinfo=None),
    }


def generate_changes(
    columns: Sequence[Column],
    keys: Sequence[str],
    state: dict[Key, Row],
    mix: ChangeMix,
    ts: datetime,
    rng: random.Random,
    *,
    violation_rate: float = 0.0,
) -> list[Row]:
    """One Qlik change batch against ``state`` (mutated in place to the post-batch state)."""
    out: list[Row] = []
    counter = 0

    def emit(op: str, row: Row, *, same_seq: str | None = None) -> str:
        nonlocal counter
        if same_seq is None:
            counter += 1
            seq = change_seq(ts, counter)
        else:
            seq = same_seq
        out.append({**{c.name: row[c.name] for c in columns}, **_header(op, seq, ts, counter)})
        return seq

    existing = sorted(state)
    rng.shuffle(existing)
    updates, rest = existing[: mix.updates], existing[mix.updates :]
    deletes = rest[: mix.deletes]

    for k in updates:
        before = state[k]
        after = {**before, **_values(columns, rng, violation_rate)}
        out.append({**{c.name: before[c.name] for c in columns}, **_header("B", "", ts, counter + 1)})
        seq = emit("U", after)
        out[-2][SEQ] = seq  # before image shares its update's sequence
        out[-2]["header__stream_position"] = seq[-12:]
        state[k] = after
    for k in deletes:
        emit("D", state.pop(k))
    next_index = 1 + max((int(k[0][4:]) if isinstance(k[0], str) else int(k[0]) for k in state), default=0)
    for i in range(mix.inserts):
        row = {c.name: _key_value(c, next_index + i) for c in columns if c.primary_key_position is not None}
        row.update(_values(columns, rng, violation_rate))
        emit("I", row)
        state[_key_of(row, keys)] = {c.name: row[c.name] for c in columns}
    return out


def apply_changes(
    full_load: Iterable[Row],
    changes: Iterable[Row],
    keys: Sequence[str],
    *,
    reload_cutoff: str | None = None,
    changes_after: datetime | None = None,
) -> dict[Key, Row]:
    """Expected SCD1 silver state (the current rows of SCD2) after applying the batches."""
    state = {_key_of(r, keys): {k: v for k, v in r.items() if not k.startswith("header__")} for r in full_load}
    ordered = sorted(
        (r for r in changes if r[OPER] != "B"),
        key=lambda r: r[SEQ],
    )
    for r in ordered:
        if reload_cutoff and r[SEQ] < reload_cutoff:
            continue
        if changes_after and r["header__timestamp"].replace(tzinfo=None) < changes_after.replace(tzinfo=None):
            continue
        k = _key_of(r, keys)
        if r[OPER] == "D":
            state.pop(k, None)
        else:
            state[k] = {c: v for c, v in r.items() if not c.startswith("header__")}
    return state
