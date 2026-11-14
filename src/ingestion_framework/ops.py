"""Operational verbs behind `make start | stop | reload` (ids come from `bundle summary`).

start   continuous pipelines: start an update; triggered groups: run their trigger job once
        (file-arrival triggers only fire for NEW files, so this drains anything already landed)
stop    stop every framework pipeline in the target
reload  after a Qlik table reload at SINCE (yyyyMMddHHmmss): record the cutoff and rebuild
        that table's silver from bronze (bronze is never reset, so this also works in prod)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

from ingestion_framework.bundle.generate import pipeline_key
from ingestion_framework.config import ConfigError, ResolvedConfig, load_config
from ingestion_framework.governance import registry_rows
from ingestion_framework.governance.job import RELOAD_CUTOFFS
from ingestion_framework.prereqs.checks import Context, resolve_warehouse
from ingestion_framework.prereqs.cli import Cli, CliError, DatabricksCli

CUTOFF = re.compile(r"^\d{14}$")


def _summary(cli: Cli, env: str, bundle_vars: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = cli.json("bundle", "summary", "-t", env, *bundle_vars)
    return out


def _ids(summary: dict[str, Any], kind: str) -> dict[str, str]:
    return {k: str(v.get("id", "")) for k, v in (summary.get("resources", {}).get(kind) or {}).items() if v.get("id")}


def start(cfg: ResolvedConfig, cli: Cli, summary: dict[str, Any]) -> list[str]:
    pipelines, jobs = _ids(summary, "pipelines"), _ids(summary, "jobs")
    done = []
    for g in cfg.groups_in_scope():
        cadence = cfg.cadence_for(g.source, g.group)
        if cadence.mode == "continuous":
            pid = pipelines.get(pipeline_key(g.source, g.group))
            if pid:
                name = cfg.pipeline_name(g.source, g.group)
                try:
                    cli.run("pipelines", "start-update", pid)
                    done.append(f"started continuous pipeline {name}")
                except CliError as exc:
                    # Deploying a pipeline as continuous starts it; that is the state we want.
                    if "active update" not in str(exc):
                        raise
                    done.append(f"continuous pipeline {name} is already running")
        else:
            jid = jobs.get(f"job_{g.source}_{g.group}_trigger")
            if jid:
                cli.run("jobs", "run-now", jid, "--no-wait")
                done.append(f"ran {cadence.name} trigger job for {cfg.pipeline_name(g.source, g.group)}")
    return done


def stop(cfg: ResolvedConfig, cli: Cli, summary: dict[str, Any]) -> list[str]:
    done = []
    for key, pid in _ids(summary, "pipelines").items():
        cli.run("pipelines", "stop", pid, "--no-wait")
        done.append(f"stopping {key}")
    return done


def reload(cfg: ResolvedConfig, cli: Cli, summary: dict[str, Any], *, table: str, since: str) -> list[str]:
    if not CUTOFF.match(since):
        raise ConfigError(f"SINCE must be the Qlik reload start as yyyyMMddHHmmss (UTC), got {since!r}")
    matches = [
        r
        for r in registry_rows(cfg, git_commit="-")
        if r["layer"] == "silver"
        and table.lower() in {r["table_fqn"].lower(), r["table_name"], r["source_table"].lower()}
    ]
    if len(matches) != 1:
        raise ConfigError(f"TABLE={table!r} must match exactly one declared silver table (got {len(matches)})")
    row = matches[0]
    ctx = Context(cfg=cfg, cli=cli)
    wh = resolve_warehouse(ctx)
    if not wh:
        raise ConfigError("reload needs the alerts warehouse (alerts.warehouse_name) to record the cutoff")
    ops = cfg.ops_schema_fqn
    statement = (
        f"MERGE INTO {ops}.{RELOAD_CUTOFFS} t USING (SELECT '{row['table_fqn']}' AS table_fqn, '{since}' AS "
        f"min_change_seq) s ON t.table_fqn = s.table_fqn WHEN MATCHED THEN UPDATE SET t.min_change_seq = "
        f"s.min_change_seq, t.set_at = current_timestamp(), t.set_by = current_user(), t.reason = 'qlik reload' "
        f"WHEN NOT MATCHED THEN INSERT (table_fqn, min_change_seq, reason, set_by, set_at) VALUES "
        f"(s.table_fqn, s.min_change_seq, 'qlik reload', current_user(), current_timestamp())"
    )
    result = cli.json(
        "api",
        "post",
        "/api/2.0/sql/statements",
        "--json",
        json.dumps({"warehouse_id": wh["id"], "statement": statement, "wait_timeout": "50s"}),
    )
    state = (result.get("status") or {}).get("state")
    if state != "SUCCEEDED":
        raise CliError(f"recording the cutoff failed: {result.get('status')}")
    pid = _ids(summary, "pipelines").get(pipeline_key(row["source"], row["group_name"]))
    if not pid:
        raise ConfigError(f"pipeline for {row['pipeline_name']} is not deployed in this target")
    cli.run("pipelines", "start-update", pid, "--json", json.dumps({"full_refresh_selection": [row["table_fqn"]]}))
    return [
        f"recorded cutoff {since} for {row['table_fqn']}",
        f"started {row['pipeline_name']} with full refresh of {row['table_fqn']} (bronze untouched)",
    ]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["start", "stop", "reload"])
    ap.add_argument("--env", required=True)
    ap.add_argument("--root", type=Path, default=Path.cwd())
    ap.add_argument("--table")
    ap.add_argument("--since")
    ap.add_argument("--var", action="append", default=[], help="bundle --var passthrough")
    args = ap.parse_args(argv)
    try:
        cfg = load_config(args.root, args.env)
        cli = DatabricksCli()
        summary = _summary(cli, args.env, [f"--var={v}" for v in args.var])
        if args.command == "start":
            lines = start(cfg, cli, summary)
        elif args.command == "stop":
            lines = stop(cfg, cli, summary)
        else:
            if not (args.table and args.since):
                raise ConfigError("reload needs TABLE=<table> and SINCE=<yyyyMMddHHmmss>")
            lines = reload(cfg, cli, summary, table=args.table, since=args.since)
    except (ConfigError, CliError) as exc:
        print(f"ops: {exc}", file=sys.stderr)
        return 2
    print("\n".join(lines) or "nothing to do")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
