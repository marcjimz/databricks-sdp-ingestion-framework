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
from ingestion_framework.governance.reload import read_reload_cutoffs  # noqa: E402

cfg = load_config(ROOT, _conf("framework.env"))
assert_fingerprint(cfg, _conf("framework.config_fingerprint"))  # same config as deploy time, or stop
source, group_name = _conf("framework.source"), _conf("framework.group")
group = cfg.group(source, group_name)

lookback = _conf("framework.ct_lookback_days")
options = RuntimeOptions(
    ct_lookback_days=int(lookback) if lookback else None,
    now=datetime.now(UTC),
    # Read once, when the graph is built. Fails the update rather than silently dropping a cutoff.
    reload_cutoffs=read_reload_cutoffs(spark, f"{cfg.ops_schema_fqn}.{RELOAD_CUTOFFS}"),
)
get_adapter(cfg.source(source).type).define(dp, spark, cfg, group, options)
