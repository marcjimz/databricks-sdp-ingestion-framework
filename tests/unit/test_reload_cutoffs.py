from __future__ import annotations

import pytest

from ingestion_framework.governance.reload import read_reload_cutoffs

TABLE = "bronze_sandbox.ingest_ops.reload_cutoffs"


class _Frame:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class FakeSpark:
    def __init__(self, rows=None, error: Exception | None = None):
        self.rows, self.error, self.queries = rows or [], error, []

    def sql(self, query):
        self.queries.append(query)
        if self.error:
            raise self.error
        return _Frame(self.rows)


def test_recorded_cutoffs_are_returned_per_silver_table():
    spark = FakeSpark(rows=[{"table_fqn": "silver.facets.t1", "min_change_seq": "20261114192956"}])
    assert read_reload_cutoffs(spark, TABLE) == {"silver.facets.t1": "20261114192956"}
    assert TABLE in spark.queries[0]


def test_a_missing_table_means_no_reloads_yet():
    spark = FakeSpark(error=Exception(f"[TABLE_OR_VIEW_NOT_FOUND] The table or view `{TABLE}` cannot be found."))
    assert read_reload_cutoffs(spark, TABLE) == {}


def test_any_other_error_stops_the_update():
    """Building silver without a recorded cutoff silently resurrects rows deleted before the reload."""
    spark = FakeSpark(error=Exception("[INSUFFICIENT_PERMISSIONS] User does not have SELECT"))
    with pytest.raises(RuntimeError, match="reload cutoffs"):
        read_reload_cutoffs(spark, TABLE)
