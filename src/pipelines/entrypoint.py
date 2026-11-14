"""Generic SDP entrypoint: every framework pipeline runs this one file.

The pipeline's configuration (set by the bundle) says which group to build:
    framework.root              bundle files root in the workspace
    framework.env               sandbox | dev | tst | prod
    framework.source            e.g. facets
    framework.group             e.g. claims
    framework.ct_lookback_days  optional; only change files modified in the last N days

Traceability: pipeline tag ingest_config -> sources/<source>/groups/<group>/ (what is built),
ingest_code -> this file + the source adapter (how it is built).
"""

import sys
from datetime import UTC, datetime

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def _conf(key: str, default: str = "") -> str:
    value = spark.conf.get(key, default)
    return value if value is not None else default


ROOT = _conf("framework.root")
if f"{ROOT}/src" not in sys.path:
    sys.path.insert(0, f"{ROOT}/src")

from ingestion_framework.adapters import RuntimeOptions, get_adapter  # noqa: E402
from ingestion_framework.config import load_config  # noqa: E402
from ingestion_framework.config.fingerprint import assert_fingerprint  # noqa: E402
from ingestion_framework.governance.job import RELOAD_CUTOFFS  # noqa: E402

cfg = load_config(ROOT, _conf("framework.env"))
assert_fingerprint(cfg, _conf("framework.config_fingerprint"))  # same config as deploy time, or stop
source, group_name = _conf("framework.source"), _conf("framework.group")
group = cfg.group(source, group_name)


def _reload_cutoffs() -> dict[str, str]:
    """Qlik reload cutoffs recorded by `make reload` (read once, when the graph is built)."""
    table = f"{cfg.ops_schema_fqn}.{RELOAD_CUTOFFS}"
    try:
        if not spark.catalog.tableExists(table):
            return {}
        rows = spark.table(table).select("table_fqn", "min_change_seq").collect()
        return {r["table_fqn"]: r["min_change_seq"] for r in rows}
    except Exception as exc:  # never block ingestion on the optional cutoff table
        print(f"[ingestion_framework] reload cutoffs unavailable ({exc}); continuing without", file=sys.stderr)
        return {}


lookback = _conf("framework.ct_lookback_days")
options = RuntimeOptions(
    ct_lookback_days=int(lookback) if lookback else None,
    now=datetime.now(UTC),
    reload_cutoffs=_reload_cutoffs(),
)
get_adapter(cfg.source(source).type).define(dp, spark, cfg, group, options)
