"""Sandbox only: write Qlik Replicate-shaped Parquet files for one Facets table.

Qlik Replicate (Store Changes on, Parquet output) writes, per table:
  <landing>/<TABLE>/LOAD00000001.parquet          the full load, no header columns
  <landing>/<TABLE>__ct/<yyyymmdd-hhmmssfff>.parquet  changes, with header__change_seq / header__change_oper
and a .dfm metadata file next to every data file (the pipeline ignores those).

Columns and types come from the bronze schema files (src/dataflows/bronze/schemas), so the generator always
matches the data flow spec. The first column is the table's key. Values are deterministic, so every run of
the same batch produces the same rows.

  --batch full      LOAD00000001.parquet with --rows members (keys 1..rows)
  --batch changes   the next change file: 10 updates (each with its before-image B), 5 deletes, 5 inserts
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import tempfile
import zlib
from pathlib import Path

import pandas as pd

LAST = ["Smith", "Johnson", "Lee", "Garcia", "Brown", "Nguyen", "Davis", "Martinez", "Clark", "Young"]
FIRST = ["Avery", "Jordan", "Riley", "Casey", "Morgan", "Taylor", "Quinn", "Jamie", "Rowan", "Skyler"]
UPDATES, DELETES, INSERTS = 10, 5, 5


def _h(*parts: object) -> int:
    return zlib.crc32("|".join(map(str, parts)).encode())


def _value(col: str, dtype: str, key: int, version: int) -> object:
    """A stable value for (column, key, version); version 0 is the full load."""
    h = _h(col, key, version)
    if dtype == "integer":
        if col.endswith("_SFX"):
            return 0
        return key if col.endswith("_CK") and col.startswith(("MEME", "SBSB")) else 1000 + h % 50
    if dtype == "timestamp":
        return pd.Timestamp("1940-01-01") + pd.Timedelta(days=h % 27000)
    if "LAST_NAME" in col:
        return LAST[h % len(LAST)]
    if "FIRST_NAME" in col:
        return FIRST[h % len(FIRST)]
    if col.endswith("_SEX"):
        return "MF"[h % 2]
    return f"{col[-4:]}{h % 1000:03d}"


def _rows(fields: list[dict], keys: list[int], version: int) -> pd.DataFrame:
    data = {f["name"]: [_value(f["name"], f["type"], k, version) for k in keys] for f in fields}
    data[fields[0]["name"]] = keys
    df = pd.DataFrame(data)
    for f in fields:
        if f["type"] == "integer":
            df[f["name"]] = df[f["name"]].astype("int32")
        elif f["type"] == "timestamp":
            # UTC-adjusted, so Spark reads them as TIMESTAMP (naive values would read as TIMESTAMP_NTZ)
            df[f["name"]] = pd.to_datetime(df[f["name"]]).astype("datetime64[us]").dt.tz_localize("UTC")
    return df


def _write(df: pd.DataFrame, target: Path) -> None:
    """Write locally, then copy into the volume, with the .dfm Qlik writes beside each data file."""
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / target.name
        df.to_parquet(local, index=False)
        shutil.copyfile(local, target)
    dfm = {"dataInfo": {"columns": [{"name": c, "type": str(t)} for c, t in df.dtypes.items()]}}
    target.with_suffix(".dfm").write_text(json.dumps(dfm))
    print(f"wrote {len(df)} rows to {target}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--landing", required=True, help="group folder, e.g. /Volumes/<catalog>/<schema>/<volume>/facets/membership")
    ap.add_argument("--schemas", required=True, help="folder holding <table>_schema.json")
    ap.add_argument("--table", required=True)
    ap.add_argument("--batch", choices=["full", "changes"], required=True)
    ap.add_argument("--rows", type=int, default=100)
    args = ap.parse_args()

    fields = json.loads((Path(args.schemas) / f"{args.table.lower()}_schema.json").read_text())["fields"]
    landing = Path(args.landing)
    if args.batch == "full":
        _write(_rows(fields, list(range(1, args.rows + 1)), 0), landing / args.table / "LOAD00000001.parquet")
        return

    ct = landing / f"{args.table}__ct"
    n = len(list(ct.glob("*.parquet"))) + 1 if ct.exists() else 1  # this is change batch n
    base = (n - 1) * (UPDATES + DELETES)
    upd = list(range(base + 1, base + UPDATES + 1))
    dele = list(range(base + UPDATES + 1, base + UPDATES + DELETES + 1))
    ins = list(range(args.rows + (n - 1) * INSERTS + 1, args.rows + n * INSERTS + 1))
    if max(dele) > args.rows:
        raise SystemExit(f"batch {n} would touch keys beyond the full load of {args.rows}; re-run the full load first")

    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    parts = []
    for oper, keys, version in (("B", upd, 0), ("U", upd, n), ("D", dele, 0), ("I", ins, n)):
        part = _rows(fields, keys, version)
        part.insert(0, "header__change_oper", oper)
        parts.append(part)
    df = pd.concat(parts, ignore_index=True)
    # A B row shares its sequence with the U that follows it; everything else gets its own.
    seq = {("B", k): i for i, k in enumerate(upd)} | {("U", k): i for i, k in enumerate(upd)}
    df.insert(0, "header__change_seq", [
        f"{n:020d}{seq.get((o, k), UPDATES + j):015d}" for j, (o, k) in enumerate(zip(df.header__change_oper, df[fields[0]["name"]]))
    ])
    df.insert(2, "header__timestamp", pd.Timestamp(now, tz="UTC").as_unit("us"))
    _write(df, ct / f"{now:%Y%m%d-%H%M%S}{now.microsecond // 1000:03d}.parquet")


if __name__ == "__main__":
    main()
