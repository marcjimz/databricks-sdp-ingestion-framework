"""Repo lint: the declarative configuration must be complete, valid and enforceable.

Run by ``make lint``, pre-commit and the test suite. Exit code 1 when any error is found.

Rules
  * every ``<table>.yml`` has a ``<table>.odcs.yaml`` data contract, and no contract is orphaned
  * contracts validate against ODCS v3.2.0 and the house profile (status, team, SLA, classification)
  * contract ``physicalName`` matches the table config; one schema object per contract
  * CDC tables declare a primary key (or are explicitly ``append_only``)
  * every quality rule is enforceable (``type: custom, engine: databricks-sdp``) and never ``drop``
  * every column maps to a Spark type
  * table names unique per source; groups within the per-pipeline table cap
  * source types are known adapters; scope and cadence references resolve in every environment
  * triggered cadences never idle longer than the file-events retention (7 days)
  * discovery mode is event-driven (file events)
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ingestion_framework.adapters import get_adapter, known_types
from ingestion_framework.adapters.qlik_cdc_files.builders import spark_type
from ingestion_framework.config import ConfigError, available_envs, load_config, read_yaml
from ingestion_framework.config.loader import CONTRACT_SUFFIX, GROUP_FILE
from ingestion_framework.contracts import unsupported_rules
from ingestion_framework.contracts.validate import profile_problems, schema_problems


@dataclass(frozen=True)
class Problem:
    path: str
    message: str
    severity: str = "error"  # error | warning


def _rel(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path)


def _lint_files(root: Path, policy: dict[str, Any]) -> list[Problem]:
    """File-level rules that must work even when the config cannot be loaded."""
    problems: list[Problem] = []
    for gdir in sorted((root / "sources").glob("*/groups/*")):
        if not gdir.is_dir():
            continue
        if not (gdir / GROUP_FILE).exists():
            problems.append(Problem(_rel(root, gdir), f"group folder has no {GROUP_FILE}"))
        configs = {p.name[: -len(".yml")] for p in gdir.glob("*.yml") if p.name != GROUP_FILE}
        contracts = {p.name[: -len(CONTRACT_SUFFIX)] for p in gdir.glob(f"*{CONTRACT_SUFFIX}")}
        for stem in sorted(configs - contracts):
            problems.append(
                Problem(
                    _rel(root, gdir / f"{stem}.yml"),
                    f"table has no data contract ({stem}{CONTRACT_SUFFIX}); every table must have one",
                )
            )
        for stem in sorted(contracts - configs):
            problems.append(
                Problem(
                    _rel(root, gdir / f"{stem}{CONTRACT_SUFFIX}"),
                    f"orphan contract: no {stem}.yml declares this table",
                )
            )
        for stem in sorted(configs & contracts):
            cpath = gdir / f"{stem}{CONTRACT_SUFFIX}"
            try:
                doc = read_yaml(cpath)
                table_doc = read_yaml(gdir / f"{stem}.yml")
            except ConfigError as exc:
                problems.append(Problem(_rel(root, cpath), str(exc)))
                continue
            for msg in schema_problems(doc) + profile_problems(doc) + unsupported_rules(doc, policy):
                problems.append(Problem(_rel(root, cpath), msg))
            objects = doc.get("schema") or []
            physical = objects[0].get("physicalName") if len(objects) == 1 else None
            if physical and physical != table_doc.get("table"):
                problems.append(
                    Problem(
                        _rel(root, cpath),
                        f"physicalName '{physical}' does not match table '{table_doc.get('table')}' in {stem}.yml",
                    )
                )
            if str(table_doc.get("table", "")).lower() != stem and not table_doc.get("name"):
                problems.append(
                    Problem(
                        _rel(root, gdir / f"{stem}.yml"),
                        f"file name should be '{str(table_doc.get('table', '')).lower()}.yml'",
                    )
                )
    return problems


def _lint_env(root: Path, env: str) -> list[Problem]:
    problems: list[Problem] = []
    env_path = f"conf/environments/{env}.yml"
    try:
        cfg = load_config(root, env)
    except ConfigError as exc:
        return [Problem(env_path, str(exc))]

    fw = cfg.framework
    if fw.section("discovery").get("mode") != "file_events":
        problems.append(Problem("conf/framework.yml", "discovery.mode must be file_events (event-driven only)"))
    cap = int(fw.limits["max_tables_per_group"])
    gap_limit = float(fw.limits["max_triggered_gap_days"])

    declared_groups = {g.key for s in cfg.sources for g in s.groups}
    declared_tables = {t.key for s in cfg.sources for g in s.groups for t in g.tables}
    for kind, declared in (
        ("sources", {s.source for s in cfg.sources}),
        ("groups", declared_groups),
        ("tables", declared_tables),
    ):
        for ref in cfg.env.scope.get(kind) or []:
            if ref not in declared:
                problems.append(Problem(env_path, f"scope.{kind} references undeclared '{ref}'"))
    for ref in cfg.env.cadence.get("overrides") or {}:
        if ref not in declared_groups:
            problems.append(Problem(env_path, f"cadence.overrides references undeclared group '{ref}'"))

    for src in cfg.sources:
        if src.type not in known_types():
            problems.append(
                Problem(
                    _rel(root, src.path / "source.yml"),
                    f"unknown source type '{src.type}' (known: {', '.join(known_types())})",
                )
            )
            continue
        adapter = get_adapter(src.type)
        names = Counter(t.name for g in src.groups for t in g.tables)
        for name, count in names.items():
            if count > 1:
                problems.append(Problem(_rel(root, src.path), f"duplicate table name '{name}' in {count} groups"))
        for g in src.groups:
            if len(g.tables) > cap:
                problems.append(
                    Problem(
                        _rel(root, g.path),
                        f"{len(g.tables)} tables exceeds max_tables_per_group ({cap}); split the group",
                    )
                )
            try:
                cadence = cfg.cadence_for(g.source, g.group)
            except ConfigError as exc:
                problems.append(Problem(env_path, f"{g.key}: {exc}"))
                continue
            gap = cadence.max_gap_days()
            if cadence.mode == "cron" and gap is None:
                problems.append(
                    Problem("conf/framework.yml", f"cron cadence '{cadence.name}' must declare max_gap_days")
                )
            if gap is not None and gap > gap_limit:
                problems.append(
                    Problem(
                        env_path,
                        f"{g.key}: cadence '{cadence.name}' can idle {gap:g} days "
                        f"> {gap_limit:g}; file events expire and Auto Loader falls back to "
                        "directory listing",
                    )
                )
            if cadence.mode == "file_arrival" and adapter.trigger_url(cfg, g) is None:
                problems.append(Problem(env_path, f"{g.key}: '{src.type}' sources cannot use a file_arrival cadence"))
            for t in g.tables:
                where = _rel(root, t.contract_path)
                if t.silver and not t.append_only and not t.contract.keys:
                    problems.append(
                        Problem(
                            where,
                            "CDC table has no primary key in its contract "
                            "(set primaryKey on the key columns, or append_only: true)",
                        )
                    )
                for c in t.contract.columns:
                    try:
                        spark_type(c)
                    except ValueError as exc:
                        problems.append(Problem(where, str(exc)))
    return problems


SECRET_KEY = ("secret", "token", "password", "client_secret", "private_key")


def _secret_keys(node: Any, path: str = "") -> list[str]:
    found: list[str] = []
    if isinstance(node, dict):
        for k, v in node.items():
            here = f"{path}.{k}" if path else str(k)
            if any(s in str(k).lower() for s in SECRET_KEY):
                found.append(here)
            found += _secret_keys(v, here)
    return found


def _lint_local_overrides(root: Path) -> list[Problem]:
    problems = []
    for p in sorted((root / "conf" / "environments").glob("*.local.yml")):
        for key in _secret_keys(read_yaml(p)):
            problems.append(
                Problem(
                    _rel(root, p),
                    f"'{key}' looks like a secret; local overrides are deployed "
                    "with the bundle, keep secrets in a CLI profile or secret scope",
                )
            )
    return problems


def lint_repo(root: Path | str) -> list[Problem]:
    root = Path(root)
    policy = dict(read_yaml(root / "conf" / "framework.yml").get("expectations") or {})
    problems = _lint_files(root, policy) + _lint_local_overrides(root)
    seen = {(p.path, p.message) for p in problems}
    for env in available_envs(root):
        for p in _lint_env(root, env):
            if (p.path, p.message) not in seen:
                seen.add((p.path, p.message))
                problems.append(p)
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Lint the ingestion framework configuration.")
    ap.add_argument("--root", type=Path, default=Path.cwd())
    args = ap.parse_args(argv)
    problems = lint_repo(args.root)
    errors = [p for p in problems if p.severity == "error"]
    for p in problems:
        print(f"{p.severity.upper():7} {p.path}: {p.message}")
    tables = sum(1 for _ in Path(args.root).glob(f"sources/*/groups/*/*{CONTRACT_SUFFIX}"))
    print(f"lint: {tables} contracts checked, {len(errors)} error(s), {len(problems) - len(errors)} warning(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
