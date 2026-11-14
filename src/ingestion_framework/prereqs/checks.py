"""Preflight: verify every declared prerequisite (conf/prerequisites.yml) for one environment.

Read-only. ``make deploy`` runs it in strict mode and stops while any *required* check fails;
``make validate`` / ``start`` run it in warn mode so drift is visible on every invocation.
"""

from __future__ import annotations

import json
import re
import shutil
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ingestion_framework.config import ResolvedConfig, read_yaml
from ingestion_framework.governance.tags import governed_tag_values
from ingestion_framework.prereqs.cli import Cli, CliError

MIN_CLI = (1, 24, 0)
MIN_PYTHON = (3, 11)
PLACEHOLDER = re.compile(r"<[^<>]+>")


@dataclass(frozen=True)
class CheckResult:
    id: str
    status: str  # pass | fail | warn | skip
    detail: str
    severity: str  # required | recommended | skip
    title: str = ""
    owner: str = ""
    fix: str = ""

    @property
    def blocking(self) -> bool:
        return self.status == "fail" and self.severity == "required"


@dataclass
class Context:
    cfg: ResolvedConfig
    cli: Cli
    identity: str = ""
    host: str = ""

    @property
    def principal(self) -> str:
        """Who must hold the privileges: the pinned deployer SP, else the deploying identity."""
        return self.cfg.env.deployer_service_principal or self.identity

    @property
    def landing_roots(self) -> dict[str, str]:
        return {
            s.source: self.cfg.landing_root(s.source)
            for s in self.cfg.sources_in_scope()
            if s.type != "lakeflow_connect"
        }


Outcome = tuple[bool, str]  # (ok, detail)


def _privileges(payload: Mapping[str, Any]) -> set[str]:
    privs: set[str] = set()
    for assignment in payload.get("privilege_assignments") or []:
        for p in assignment.get("privileges") or []:
            privs.add(str(p.get("privilege")))
    return privs


def _has(privs: set[str], needed: list[str]) -> list[str]:
    if "ALL_PRIVILEGES" in privs:
        return []
    return [n for n in needed if n not in privs]


def _placeholders(node: Any, path: str = "") -> Iterator[str]:
    if isinstance(node, Mapping):
        for k, v in node.items():
            if k == "storage":  # only used by `make prereqs`
                continue
            yield from _placeholders(v, f"{path}.{k}" if path else str(k))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from _placeholders(v, f"{path}[{i}]")
    elif isinstance(node, str) and PLACEHOLDER.search(node):
        yield f"{path} = {node}"


def _version(text: str) -> tuple[int, ...]:
    m = re.search(r"v?(\d+)\.(\d+)\.(\d+)", text)
    return tuple(int(x) for x in m.groups()) if m else (0, 0, 0)


def find_external_location(locations: list[Mapping[str, Any]], url: str) -> Mapping[str, Any] | None:
    """Longest external-location URL that is a prefix of ``url``."""
    target = url.rstrip("/") + "/"
    best: Mapping[str, Any] | None = None
    for loc in locations:
        prefix = str(loc.get("url", "")).rstrip("/") + "/"
        if target.startswith(prefix) and (best is None or len(prefix) > len(str(best["url"]).rstrip("/")) + 1):
            best = loc
    return best


# ---- checks -----------------------------------------------------------------------------
def check_local_tools(ctx: Context) -> Outcome:
    problems = []
    cli_version = _version(ctx.cli.text("version"))
    if cli_version < MIN_CLI:
        problems.append(f"Databricks CLI {'.'.join(map(str, cli_version))} < {'.'.join(map(str, MIN_CLI))}")
    if sys.version_info < MIN_PYTHON:
        problems.append(f"Python {sys.version.split()[0]} < {'.'.join(map(str, MIN_PYTHON))}")
    if not shutil.which("uv"):
        problems.append("uv not on PATH")
    detail = "; ".join(problems) or f"CLI {'.'.join(map(str, cli_version))}, Python {sys.version.split()[0]}"
    return not problems, detail


def check_env_placeholders(ctx: Context) -> Outcome:
    found = list(_placeholders(ctx.cfg.env.raw))
    return not found, "; ".join(found) or "no placeholders"


def check_workspace_host(ctx: Context) -> Outcome:
    expected = ctx.cfg.env.workspace_host.rstrip("/")
    if not expected or PLACEHOLDER.search(expected):
        return True, f"not pinned (PROFILE host {ctx.host})"
    return expected == ctx.host.rstrip("/"), f"PROFILE host {ctx.host}, expected {expected}"


def check_deployer_identity(ctx: Context) -> Outcome:
    expected = ctx.cfg.env.deployer_service_principal
    if not expected or PLACEHOLDER.search(expected):
        return False, f"deploying as {ctx.identity}; no deployer service principal pinned"
    return ctx.identity == expected, f"deploying as {ctx.identity}, expected {expected}"


def check_catalogs(ctx: Context) -> Outcome:
    missing = []
    for layer in ("bronze", "silver"):
        name = ctx.cfg.catalog(layer)
        try:
            ctx.cli.json("catalogs", "get", name)
        except CliError:
            missing.append(name)
    return not missing, f"missing: {', '.join(missing)}" if missing else "bronze + silver exist"


def check_catalog_privileges(ctx: Context) -> Outcome:
    gaps = []
    for layer in ("bronze", "silver"):
        name = ctx.cfg.catalog(layer)
        try:
            owner = ctx.cli.json("catalogs", "get", name).get("owner")
            if owner == ctx.principal:
                continue
            privs = _privileges(ctx.cli.json("grants", "get-effective", "catalog", name, "--principal", ctx.principal))
        except CliError as exc:
            gaps.append(f"{name}: {exc}")
            continue
        missing = _has(privs, ["USE_CATALOG", "CREATE_SCHEMA"])
        if missing:
            gaps.append(f"{name}: {ctx.principal} lacks {', '.join(missing)}")
    return not gaps, "; ".join(gaps) or f"{ctx.principal} can create schemas"


def _locations(ctx: Context) -> dict[str, Mapping[str, Any] | None]:
    locations = ctx.cli.json("external-locations", "list")
    locations = locations if isinstance(locations, list) else locations.get("external_locations", [])
    return {src: find_external_location(locations, url) for src, url in ctx.landing_roots.items()}


def check_landing_external_location(ctx: Context) -> Outcome:
    found = _locations(ctx)
    missing = [f"{src} ({ctx.landing_roots[src]})" for src, loc in found.items() if loc is None]
    ok = ", ".join(f"{src} -> {loc['name']}" for src, loc in found.items() if loc)
    return not missing, f"no external location covers: {', '.join(missing)}" if missing else ok


def _uncovered(found: Mapping[str, Mapping[str, Any] | None]) -> list[str]:
    return [src for src, loc in found.items() if loc is None]


def check_file_events(ctx: Context) -> Outcome:
    found = _locations(ctx)
    if uncovered := _uncovered(found):
        return False, f"cannot verify: no external location covers {', '.join(uncovered)}"
    off = [f"{loc['name']}" for loc in found.values() if loc and not loc.get("enable_file_events")]
    return not off, f"file events OFF on: {', '.join(off)}" if off else "file events enabled"


def check_landing_privileges(ctx: Context) -> Outcome:
    needed = ["READ_FILES"] + (["WRITE_FILES"] if ctx.cfg.env.seed_enabled else [])
    found = _locations(ctx)
    if uncovered := _uncovered(found):
        return False, f"cannot verify: no external location covers {', '.join(uncovered)}"
    gaps = []
    for loc in {id(v): v for v in found.values() if v}.values():
        if loc.get("owner") == ctx.principal:
            continue
        privs = _privileges(
            ctx.cli.json("grants", "get-effective", "external_location", str(loc["name"]), "--principal", ctx.principal)
        )
        missing = _has(privs, needed)
        if missing:
            gaps.append(f"{loc['name']}: {ctx.principal} lacks {', '.join(missing)}")
    return not gaps, "; ".join(gaps) or f"{ctx.principal} has {', '.join(needed)}"


def resolve_warehouse(ctx: Context) -> Mapping[str, Any] | None:
    name = str(ctx.cfg.env.alerts.get("warehouse_name", ""))
    warehouses = ctx.cli.json("warehouses", "list")
    return next((w for w in warehouses if w.get("name") == name or w.get("id") == name), None)


def check_warehouse(ctx: Context) -> Outcome:
    if not ctx.cfg.env.alerts.get("enabled", True):
        return True, "alerts disabled"
    wh = resolve_warehouse(ctx)
    if not wh:
        return False, f"warehouse '{ctx.cfg.env.alerts.get('warehouse_name')}' not found"
    try:
        acl = ctx.cli.json("permissions", "get", "warehouses", str(wh["id"])).get("access_control_list") or []
    except CliError:
        # Reading the ACL needs Manage, which a deployer should not have: prove usability instead.
        probe = ctx.cli.json(
            "api",
            "post",
            "/api/2.0/sql/statements",
            "--json",
            json.dumps({"warehouse_id": wh["id"], "statement": "SELECT 1", "wait_timeout": "30s"}),
        )
        state = str((probe.get("status") or {}).get("state", ""))
        ok = state in {"SUCCEEDED", "PENDING", "RUNNING"}  # accepted = the principal may use it
        return ok, f"{wh['name']} ({wh['id']}): probe query {state}" + (
            "" if ok else f"; {ctx.principal} cannot run on it"
        )
    levels = {"CAN_USE", "CAN_MONITOR", "CAN_MANAGE", "IS_OWNER"}
    for entry in acl:
        who = entry.get("service_principal_name") or entry.get("user_name") or entry.get("group_name")
        granted = {p.get("permission_level") for p in entry.get("all_permissions") or []}
        if who in {ctx.principal, "users"} and granted & levels:
            return True, f"{wh['name']} ({wh['id']}) usable by {who}"
    return False, f"{ctx.principal} has no CAN_USE on warehouse {wh['name']}"


def check_tag_policies(ctx: Context) -> Outcome:
    if not ctx.cfg.governed_tags_enabled:
        return False, "governed_tags is false: tags are applied as plain (ungoverned) tags"
    gaps = []
    for key, values in governed_tag_values(ctx.cfg).items():
        try:
            policy = ctx.cli.json("tag-policies", "get-tag-policy", key)
        except CliError:
            gaps.append(f"{key}: policy missing (needs {', '.join(values)})")
            continue
        have = {v.get("name") for v in policy.get("values") or []}
        missing = [v for v in values if v not in have]
        if missing:
            gaps.append(f"{key}: missing values {', '.join(missing)}")
    return not gaps, "; ".join(gaps) or "all governed tag values allowed"


def resolve_user_ids(cli: Cli, emails: Sequence[str]) -> tuple[list[str], list[str]]:
    """Workspace user ids for emails (DQM notifications only accept ids). Returns (ids, unresolved)."""
    ids, unresolved = [], []
    for email in emails:
        try:
            found = cli.json("users", "list", "--filter", f'userName eq "{email}"')
        except CliError:
            found = []
        users = found if isinstance(found, list) else found.get("Resources", [])
        if users:
            ids.append(str(users[0]["id"]))
        else:
            unresolved.append(email)
    return ids, unresolved


def check_alert_recipients(ctx: Context) -> Outcome:
    env = ctx.cfg.env
    missing = []
    if not (env.alert_emails or env.alert_destination_ids):
        missing.append("alerts.notify")
    if not (env.dqm_emails or env.dqm_destination_ids):
        missing.append("dqm.notify")
    if missing:
        return False, f"no recipients in {', '.join(missing)}"
    _, unresolved = resolve_user_ids(ctx.cli, env.dqm_emails)
    if unresolved:
        return False, f"dqm.notify emails are not workspace users (DQM needs user ids): {', '.join(unresolved)}"
    return True, "recipients configured"


def check_notification_destinations(ctx: Context) -> Outcome:
    ids = set(ctx.cfg.env.alert_destination_ids) | set(ctx.cfg.env.dqm_destination_ids)
    missing = []
    for dest in sorted(ids):
        try:
            ctx.cli.json("notification-destinations", "get", dest)
        except CliError:
            missing.append(dest)
    return (
        not missing,
        f"unknown destination ids: {', '.join(missing)}" if missing else f"{len(ids)} destination(s) ok",
    )


def check_system_tables(ctx: Context) -> Outcome:
    payload = ctx.cli.json(
        "grants", "get-effective", "schema", "system.data_quality_monitoring", "--principal", ctx.principal
    )
    missing = _has(_privileges(payload), ["USE_SCHEMA", "SELECT"])
    return not missing, f"{ctx.principal} lacks {', '.join(missing)}" if missing else "readable"


def check_connections(ctx: Context) -> Outcome:
    missing = []
    for src in ctx.cfg.sources_in_scope():
        if src.type == "lakeflow_connect":
            name = str(src.settings.get("connection_name", ""))
            try:
                ctx.cli.json("connections", "get", name)
            except CliError:
                missing.append(f"{src.source}: {name}")
    return not missing, f"missing connections: {', '.join(missing)}" if missing else "ok"


def check_nonprod_data_attestation(ctx: Context) -> Outcome:
    ok = bool(ctx.cfg.env.governance.get("nonprod_data_attested", False))
    return ok, "attested" if ok else "governance.nonprod_data_attested is not set"


def _applies(check_id: str, cfg: ResolvedConfig) -> bool:
    if check_id == "notification_destinations":
        return bool(set(cfg.env.alert_destination_ids) | set(cfg.env.dqm_destination_ids))
    if check_id == "system_tables":
        return bool(cfg.env.alerts.get("dqm_system_table_alert", False))
    if check_id == "connections":
        return any(s.type == "lakeflow_connect" for s in cfg.sources_in_scope())
    return True


CHECKS: dict[str, Callable[[Context], Outcome]] = {
    "local_tools": check_local_tools,
    "env_placeholders": check_env_placeholders,
    "workspace_host": check_workspace_host,
    "deployer_identity": check_deployer_identity,
    "catalogs": check_catalogs,
    "catalog_privileges": check_catalog_privileges,
    "landing_external_location": check_landing_external_location,
    "file_events": check_file_events,
    "landing_privileges": check_landing_privileges,
    "warehouse": check_warehouse,
    "tag_policies": check_tag_policies,
    "alert_recipients": check_alert_recipients,
    "notification_destinations": check_notification_destinations,
    "system_tables": check_system_tables,
    "connections": check_connections,
    "nonprod_data_attestation": check_nonprod_data_attestation,
}


def declared_checks(cfg: ResolvedConfig) -> Mapping[str, Mapping[str, Any]]:
    checks: Mapping[str, Mapping[str, Any]] = read_yaml(cfg.root / "conf" / "prerequisites.yml")["checks"]
    return checks


def run_preflight(cfg: ResolvedConfig, cli: Cli) -> list[CheckResult]:
    declared = declared_checks(cfg)
    ctx = Context(cfg=cfg, cli=cli)
    try:
        ctx.host = str((cli.json("auth", "describe").get("details") or {}).get("host", ""))
        ctx.identity = str(cli.json("current-user", "me").get("userName", ""))
    except CliError as exc:
        return [
            CheckResult(
                "auth",
                "fail",
                f"cannot authenticate with PROFILE: {exc}",
                "required",
                "CLI authentication",
                "you",
                "databricks auth login -p <profile>",
            )
        ]

    results = []
    for check_id, meta in declared.items():
        severity = str((meta.get("severity_by_env") or {}).get(cfg.env.name, meta.get("severity", "required")))
        base = {
            "severity": severity,
            "title": str(meta.get("title", "")),
            "owner": str(meta.get("owner", "")),
            "fix": str(meta.get("fix", "")),
        }
        if severity == "skip" or not _applies(check_id, cfg):
            results.append(CheckResult(check_id, "skip", "not applicable", **base))
            continue
        fn = CHECKS.get(check_id)
        if fn is None:
            results.append(CheckResult(check_id, "fail", "declared but not implemented", **base))
            continue
        try:
            ok, detail = fn(ctx)
        except CliError as exc:
            ok, detail = False, f"check errored: {exc}"
        status = "pass" if ok else ("fail" if severity == "required" else "warn")
        results.append(CheckResult(check_id, status, detail, **base))
    return results


def bundle_vars(cfg: ResolvedConfig, cli: Cli) -> dict[str, str]:
    """Variables the bundle needs that are resolved from the workspace (passed by make as --var)."""
    ctx = Context(cfg=cfg, cli=cli)
    ctx.identity = str(cli.json("current-user", "me").get("userName", ""))
    wh = resolve_warehouse(ctx) if cfg.env.alerts.get("enabled", True) else None
    dqm_ids, _ = resolve_user_ids(cli, cfg.env.dqm_emails)
    return {
        "warehouse_id": str(wh["id"]) if wh else "",
        "deployer_sp": ctx.principal,
        "dqm_user_ids": ",".join(dqm_ids),
    }
