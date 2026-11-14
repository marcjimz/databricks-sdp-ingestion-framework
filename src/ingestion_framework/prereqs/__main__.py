"""CLI used by the Makefile.

python -m ingestion_framework.prereqs preflight   --env sandbox [--strict]
python -m ingestion_framework.prereqs apply       --env sandbox [--dry-run]
python -m ingestion_framework.prereqs bundle-vars --env sandbox
python -m ingestion_framework.prereqs host        --env sandbox   (workspace_host, for PROFILE=azure-cli)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from ingestion_framework.config import ConfigError, load_config
from ingestion_framework.prereqs import (
    CheckResult,
    CliError,
    DatabricksCli,
    apply_prereqs,
    bundle_vars,
    run_preflight,
)

ICON = {"pass": "✓", "fail": "✗", "warn": "!", "skip": "-"}


def _report(env: str, results: list[CheckResult], strict: bool) -> int:
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE") or os.environ.get("DATABRICKS_AUTH_TYPE") or "<ambient auth>"
    print(f"\nPreflight: env={env} profile={profile}  (declared in conf/prerequisites.yml)\n")
    width = max(len(r.id) for r in results)
    for r in results:
        print(f"  {ICON[r.status]} {r.id.ljust(width)}  {r.detail}")
        if r.status in {"fail", "warn"}:
            print(f"    {' ' * width}  [{r.severity}] owner: {r.owner}")
            print(f"    {' ' * width}  fix: {r.fix}")
    blocking = [r for r in results if r.blocking]
    warnings = [r for r in results if r.status == "warn"]
    print()
    if blocking:
        verdict = "deploy BLOCKED" if strict else "deploy would be blocked"
        print(f"Preflight: {len(blocking)} required prerequisite(s) not met - {verdict}.")
        print(f"Run `make prereqs {env} PROFILE=<admin>` for the automatable ones; see docs/prerequisites.md.")
    else:
        print(f"Preflight: all required prerequisites met ({len(warnings)} warning(s)).")
    return 1 if (strict and blocking) else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m ingestion_framework.prereqs")
    ap.add_argument("command", choices=["preflight", "apply", "bundle-vars", "host"])
    ap.add_argument("--env", required=True)
    ap.add_argument("--root", type=Path, default=Path.cwd())
    ap.add_argument("--strict", action="store_true", help="exit 1 when a required check fails")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--principal", help="override the deployer principal for `apply`")
    args = ap.parse_args(argv)
    try:
        cfg = load_config(args.root, args.env)
        if args.command == "host":  # used by `PROFILE=azure-cli`: no workspace call needed
            print(cfg.env.workspace_host)
            return 0 if cfg.env.workspace_host else 2
        cli = DatabricksCli()
        if args.command == "preflight":
            return _report(args.env, run_preflight(cfg, cli), args.strict)
        if args.command == "apply":
            for line in apply_prereqs(cfg, cli, dry_run=args.dry_run, principal=args.principal):
                print(line)
            return 0
        values = bundle_vars(cfg, cli)
        print(" ".join(f"--var={k}={v}" for k, v in values.items() if v))
        return 0
    except (ConfigError, CliError) as exc:
        print(f"prereqs: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
