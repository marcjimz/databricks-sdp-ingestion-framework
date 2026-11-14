"""`make trace`: walk table -> pipeline -> config -> code, the way an operator would in production.

    make trace prod TABLE=silver_prod.facets.cmc_clcl_claim PROFILE=<p>
    make trace prod TABLE=cmc_clcl_claim                     # bare names resolve too
    make trace prod PIPELINE=ingest-facets-claims-prod

The repo answers what SHOULD be true; with a working PROFILE it also reads what IS true in the
workspace (the table's ingest_pipeline tag and the pipeline's state), and flags any mismatch.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from ingestion_framework.config import ConfigError, load_config
from ingestion_framework.governance import registry_rows
from ingestion_framework.prereqs.cli import Cli, CliError, DatabricksCli


def _match(rows: list[dict[str, Any]], table: str | None, pipeline: str | None) -> list[dict[str, Any]]:
    if pipeline:
        return [r for r in rows if r["pipeline_name"] == pipeline]
    assert table
    t = table.lower()
    return [r for r in rows if t in {r["table_fqn"].lower(), r["table_name"], r["source_table"].lower()}]


def _live(cli: Cli, row: dict[str, Any], tag_key: str) -> dict[str, str]:
    info: dict[str, str] = {}
    try:
        tags = cli.json("entity-tag-assignments", "list", "tables", row["table_fqn"])
        items = tags if isinstance(tags, list) else tags.get("tag_assignments", [])
        info["tag"] = next((str(t.get("tag_value")) for t in items if t.get("tag_key") == tag_key), "<missing>")
    except CliError as exc:
        info["tag"] = f"<unavailable: {exc}>"
    try:
        found = cli.json("pipelines", "list-pipelines", "--filter", f"name LIKE '{row['pipeline_name']}'")
        found = found if isinstance(found, list) else found.get("statuses", [])
        if found:
            p = found[0]
            host = str((cli.json("auth", "describe").get("details") or {}).get("host", "")).rstrip("/")
            info["pipeline"] = f"{p.get('state', '?')}  {host}/pipelines/{p.get('pipeline_id')}"
        else:
            info["pipeline"] = "<not deployed>"
    except CliError as exc:
        info["pipeline"] = f"<unavailable: {exc}>"
    return info


def trace(root: Path, env: str, *, table: str | None, pipeline: str | None, cli: Cli | None) -> int:
    cfg = load_config(root, env)
    rows = _match(registry_rows(cfg, git_commit="(repo)"), table, pipeline)
    if not rows:
        print(f"trace: nothing declared matches {table or pipeline!r} in env '{env}'", file=sys.stderr)
        return 1
    tag_key = cfg.tag_key("ingest_pipeline")
    for r in rows:
        contract = f"{r['contract_path']}  ({r['contract_id']}@{r['contract_version']})"
        steps = [
            ("1. table tag", tag_key, r["pipeline_name"]),
            (
                "2. pipeline tag",
                cfg.tag_key("ingest_config"),
                cfg.rel(cfg.group(r["source"], r["group_name"]).path) + "/",
            ),
            ("", cfg.tag_key("ingest_code"), r["code_ref"]),
            ("3. table config", "", r["config_path"]),
            ("", "data contract", contract),
            ("", "cadence", f"{r['cadence']} ({r['cadence_mode']})"),
        ]
        print(f"\n{r['table_fqn']}  ({r['layer']})")
        for step, label, value in steps:
            print(f"  {step:<16} {label:<22} -> {value}")
        if cli:
            live = _live(cli, r, tag_key)
            flag = "" if live["tag"] == r["pipeline_name"] else "   <-- DRIFT (governance job not run, or retagged)"
            print(f"  {'live':<16} {'table tag':<22} =  {live['tag']}{flag}")
            print(f"  {'':<16} {'pipeline':<22} =  {live['pipeline']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True)
    ap.add_argument("--root", type=Path, default=Path.cwd())
    target = ap.add_mutually_exclusive_group(required=True)
    target.add_argument("--table")
    target.add_argument("--pipeline")
    ap.add_argument("--offline", action="store_true", help="repo only; do not call the workspace")
    args = ap.parse_args(argv)
    try:
        cli = None if args.offline else DatabricksCli()
        return trace(args.root, args.env, table=args.table, pipeline=args.pipeline, cli=cli)
    except (ConfigError, CliError) as exc:
        print(f"trace: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
