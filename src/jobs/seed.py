"""Job task: ingest-seed-<env> (SANDBOX ONLY). See ingestion_framework.synthetic.seed."""

import argparse
import json
import sys


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--env", required=True)
    ap.add_argument("--config-fingerprint", default="")
    ap.add_argument("--batch", required=True, choices=["full_load", "changes", "verify", "reset"])
    args = ap.parse_args()
    sys.path.insert(0, f"{args.root}/src")

    from pyspark.sql import SparkSession

    from ingestion_framework.config import load_config
    from ingestion_framework.config.fingerprint import assert_fingerprint
    from ingestion_framework.synthetic.seed import run

    spark = SparkSession.builder.getOrCreate()
    cfg = load_config(args.root, args.env)
    assert_fingerprint(cfg, args.config_fingerprint)
    print(json.dumps(run(spark, cfg, args.batch), indent=2, default=str))


if __name__ == "__main__":
    main()
