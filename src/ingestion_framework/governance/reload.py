"""Qlik reload cutoffs, read by every pipeline when its graph is built.

``make reload`` records a cutoff per silver table in ``<ops>.reload_cutoffs``. The silver view then keeps only the
snapshot written at or after it, plus changes committed at or after it (see ``silver_source_filter``).
"""

from __future__ import annotations

from typing import Any


def read_reload_cutoffs(spark: Any, table: str) -> dict[str, str]:
    """Silver table fqn -> cutoff. A missing table means no reload has been recorded yet. Any other error stops
    the update: building silver without a recorded cutoff would silently resurrect rows deleted before the
    reload."""
    try:
        rows = spark.sql(f"SELECT table_fqn, min_change_seq FROM {table}").collect()
    except Exception as exc:  # re-raised unless it is the one tolerated case
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
            return {}
        raise RuntimeError(f"cannot read reload cutoffs from {table}: {exc}") from exc
    return {str(r["table_fqn"]): str(r["min_change_seq"]) for r in rows}
