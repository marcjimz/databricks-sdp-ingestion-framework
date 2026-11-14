from __future__ import annotations

import random
from datetime import UTC, datetime

from ingestion_framework.config import Column
from ingestion_framework.synthetic import (
    ChangeMix,
    apply_changes,
    change_seq,
    generate_changes,
    generate_full_load,
)

COLS = (
    Column("K1", "string", "string", True, 1),
    Column("K2", "integer", "int", True, 2),
    Column("NAME", "string", "string", False, None),
    Column("TOTAL", "number", "decimal(19,4)", False, None),
    Column("PAID", "number", "decimal(19,4)", False, None),
    Column("BORN", "timestamp", "timestamp", False, None),
)
KEYS = ("K1", "K2")
T0 = datetime(2026, 11, 13, 10, 30, tzinfo=UTC)


def _key(row):
    return tuple(row[k] for k in KEYS)


def test_change_seq_is_35_chars_and_monotonic():
    a, b, c = change_seq(T0, 1), change_seq(T0, 2), change_seq(T0.replace(minute=31), 1)
    assert len(a) == 35 and a < b < c
    assert a.startswith("20261113103000")


def test_full_load_has_unique_keys_and_no_headers():
    rows = generate_full_load(COLS, 50, random.Random(1))
    assert len({_key(r) for r in rows}) == 50
    assert not any(k.startswith("header__") for k in rows[0])


def test_decimal_columns_are_non_increasing_left_to_right():
    """Mirrors charge >= allowed >= paid, so cross-column contract rules hold for synthetic data."""
    for row in generate_full_load(COLS, 50, random.Random(2)):
        assert row["TOTAL"] >= row["PAID"]


def test_changes_emit_before_images_with_matching_seq_and_all_ops():
    rng = random.Random(3)
    state = {_key(r): r for r in generate_full_load(COLS, 20, rng)}
    rows = generate_changes(COLS, KEYS, state, ChangeMix(inserts=3, updates=4, deletes=2), T0, rng)

    ops = [r["header__change_oper"] for r in rows]
    assert ops.count("I") == 3 and ops.count("U") == 4 and ops.count("B") == 4 and ops.count("D") == 2
    for b_row in (r for r in rows if r["header__change_oper"] == "B"):
        u_row = next(r for r in rows if r["header__change_oper"] == "U" and _key(r) == _key(b_row))
        assert u_row["header__change_seq"] == b_row["header__change_seq"]
    seqs = [r["header__change_seq"] for r in rows if r["header__change_oper"] != "B"]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)


def test_oracle_applies_inserts_updates_deletes_and_ignores_before_images():
    rng = random.Random(4)
    full = generate_full_load(COLS, 10, rng)
    state = {_key(r): dict(r) for r in full}
    changes = generate_changes(COLS, KEYS, state, ChangeMix(inserts=2, updates=3, deletes=1), T0, rng)
    final = apply_changes(full, changes, KEYS)

    assert len(final) == 10 + 2 - 1
    updated = [r for r in changes if r["header__change_oper"] == "U"]
    for u in updated:
        if _key(u) in final:
            assert final[_key(u)]["NAME"] == u["NAME"]
    deleted = [_key(r) for r in changes if r["header__change_oper"] == "D"]
    assert all(k not in final for k in deleted)
    assert not any(k.startswith("header__") for row in final.values() for k in row)


def test_cached_overlap_is_idempotent():
    """Qlik replays changes captured during the full load; applying them again changes nothing."""
    rng = random.Random(5)
    full = generate_full_load(COLS, 10, rng)
    replay = [
        {**full[0], "header__change_oper": "U", "header__change_seq": change_seq(T0, 1)},
        {**full[1], "header__change_oper": "I", "header__change_seq": change_seq(T0, 2)},
    ]
    final = apply_changes(full, replay, KEYS)
    assert final == {_key(r): r for r in full}


def test_reload_cutoff_drops_stale_changes():
    rng = random.Random(6)
    full = generate_full_load(COLS, 5, rng)
    stale_delete = {**full[0], "header__change_oper": "D", "header__change_seq": change_seq(T0, 1)}
    final = apply_changes(full, [stale_delete], KEYS, reload_cutoff=change_seq(T0.replace(hour=11), 0))
    assert _key(full[0]) in final


def test_lookback_window_selects_only_recent_change_batches():
    rng = random.Random(7)
    full = generate_full_load(COLS, 10, rng)
    state = {_key(r): dict(r) for r in full}
    old = generate_changes(COLS, KEYS, state, ChangeMix(updates=2), T0.replace(day=1), rng)
    new = generate_changes(COLS, KEYS, state, ChangeMix(updates=2), T0, rng)
    final = apply_changes(full, old + new, KEYS, changes_after=T0.replace(day=12))
    expected = apply_changes(full, new, KEYS)
    assert final == expected
