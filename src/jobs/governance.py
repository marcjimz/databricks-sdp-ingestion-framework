"""Job task: ingest-governance-<env>. See ingestion_framework.governance.job for what it does."""

import argparse
import json
import sys


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--env", required=True)
    ap.add_argument("--config-fingerprint", default="")
    ap.add_argument("--git-commit", default="unknown")
    args = ap.parse_args()
    sys.path.insert(0, f"{args.root}/src")

    from pyspark.sql import SparkSession

    from ingestion_framework.config import load_config
    from ingestion_framework.config.fingerprint import assert_fingerprint
    from ingestion_framework.governance.job import run

    spark = SparkSession.builder.getOrCreate()
    cfg = load_config(args.root, args.env)
    assert_fingerprint(cfg, args.config_fingerprint)
    summary = run(spark, cfg, git_commit=args.git_commit)
    print(json.dumps(summary, indent=2))
    if summary["tag_failures"]:
        raise SystemExit(f"{len(summary['tag_failures'])} table(s) could not be tagged")


if __name__ == "__main__":
    main()
