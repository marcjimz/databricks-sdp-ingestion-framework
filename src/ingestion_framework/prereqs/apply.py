"""`make prereqs`: apply the automatable prerequisites with an ADMIN profile (never the deployer).

Idempotent: every action is preceded by a read and skipped when already satisfied. ``--dry-run``
prints the exact CLI calls without executing them.
"""

from __future__ import annotations

import json
from typing import Any

from ingestion_framework.config import ResolvedConfig
from ingestion_framework.governance.tags import governed_tag_values
from ingestion_framework.prereqs.checks import (
    PLACEHOLDER,
    Context,
    _has,
    _locations,
    _privileges,
    check_warehouse,
    find_external_location,
    resolve_warehouse,
)
from ingestion_framework.prereqs.cli import Cli, CliError

MANUAL_STEPS = [
    "Deployer SP: grant it ASSIGN on the governed tag policies (Catalog > Governed tags > Permissions),"
    " or set governance.account_id so make prereqs grants it.",
    "Qlik Replicate: target endpoint Parquet, Store Changes on, metadata (.dfm) files on, one task per group folder.",
    "Data governance: confirm non-prod source data handling, then set governance.nonprod_data_attested: true.",
]


class _Actions:
    def __init__(self, cli: Cli, dry_run: bool) -> None:
        self.cli, self.dry_run = cli, dry_run
        self.log: list[str] = []

    def do(self, *args: str) -> None:
        self.log.append("databricks " + " ".join(args))
        if not self.dry_run:
            self.cli.run(*args)

    def note(self, message: str) -> None:
        self.log.append(f"# {message}")


def _exists(cli: Cli, *args: str) -> bool:
    try:
        cli.json(*args)
        return True
    except CliError:
        return False


def _storage(cfg: ResolvedConfig, cli: Cli, acts: _Actions) -> None:
    storage: dict[str, Any] = dict(cfg.env.storage)
    if not storage.get("create"):
        return
    unset = [k for k, v in storage.items() if isinstance(v, str) and PLACEHOLDER.search(v)]
    if unset:
        acts.note(f"storage bootstrap skipped: fill storage.{', storage.'.join(unset)} first")
        return
    cloud = str(storage.get("cloud", "aws"))
    if cloud == "azure":
        identity: dict[str, Any] = {"azure_managed_identity": {"access_connector_id": storage["access_connector_id"]}}
        queue: dict[str, Any] = {
            "managed_aqs": {"resource_group": storage["resource_group"], "subscription_id": storage["subscription_id"]}
        }
    elif cloud == "aws":
        identity = {"aws_iam_role": {"role_arn": storage["iam_role_arn"]}}
        queue = {"managed_sqs": {}}
    else:
        acts.note(f"storage bootstrap skipped: storage.cloud '{cloud}' not supported (aws | azure)")
        return
    cred, loc = str(storage["credential_name"]), str(storage["external_location_name"])
    if not _exists(cli, "storage-credentials", "get", cred):
        acts.do(
            "storage-credentials",
            "create",
            "--json",
            json.dumps({"name": cred, **identity, "comment": "ingestion framework sandbox"}),
        )
    if not _exists(cli, "external-locations", "get", loc):
        acts.do(
            "external-locations",
            "create",
            "--json",
            json.dumps(
                {
                    "name": loc,
                    "url": storage["url"],
                    "credential_name": cred,
                    "enable_file_events": True,
                    "file_event_queue": queue,
                    "comment": "ingestion framework sandbox landing (file events on)",
                }
            ),
        )
    # Catalog managed storage must be covered by an external location too. It gets its own
    # location (file events off): managed-table writes must not flood the landing event stream.
    catalog_loc = str(storage.get("catalog_external_location_name") or "")
    if catalog_loc and not _exists(cli, "external-locations", "get", catalog_loc):
        acts.do(
            "external-locations",
            "create",
            "--json",
            json.dumps(
                {
                    "name": catalog_loc,
                    "url": storage["catalog_storage_root"],
                    "credential_name": cred,
                    "enable_file_events": False,  # on by default for new locations; nothing consumes these
                    "comment": "ingestion framework sandbox catalog managed storage",
                }
            ),
        )
    for layer in ("bronze", "silver"):
        name = cfg.catalog(layer)
        if not _exists(cli, "catalogs", "get", name):
            root = str(storage["catalog_storage_root"]).rstrip("/") + f"/{name}"
            acts.do(
                "catalogs",
                "create",
                name,
                "--storage-root",
                root,
                "--comment",
                f"{layer} layer - schemas owned by the ingestion framework",
            )


def _grants(cfg: ResolvedConfig, cli: Cli, acts: _Actions, principal: str) -> None:
    ctx = Context(cfg=cfg, cli=cli, identity=principal)
    for layer in ("bronze", "silver"):
        name = cfg.catalog(layer)
        try:
            if cli.json("catalogs", "get", name).get("owner") == principal:
                continue
            privs = _privileges(cli.json("grants", "get-effective", "catalog", name, "--principal", principal))
        except CliError:
            if acts.dry_run:
                privs = set()  # catalog would be created above
            else:
                acts.note(f"catalog {name} not found; cannot grant")
                continue
        missing = _has(privs, ["USE_CATALOG", "CREATE_SCHEMA"])
        if missing:
            acts.do(
                "grants",
                "update",
                "catalog",
                name,
                "--json",
                json.dumps({"changes": [{"principal": principal, "add": missing}]}),
            )

    needed = ["READ_FILES"] + (["WRITE_FILES"] if cfg.env.seed_enabled else [])
    try:
        locations = _locations(ctx)
    except CliError as exc:
        acts.note(f"cannot list external locations: {exc}")
        locations = {}
    if cfg.env.storage.get("create") and not any(locations.values()):
        storage = cfg.env.storage
        locations = {
            "_": find_external_location(
                [{"name": storage.get("external_location_name"), "url": storage.get("url"), "owner": ""}],
                next(iter(ctx.landing_roots.values()), ""),
            )
        }
    for loc in {str(v["name"]): v for v in locations.values() if v}.values():
        if loc.get("owner") == principal:
            continue
        try:
            privs = _privileges(
                cli.json("grants", "get-effective", "external_location", str(loc["name"]), "--principal", principal)
            )
        except CliError:
            privs = set()
        missing = _has(privs, needed)
        if missing:
            acts.do(
                "grants",
                "update",
                "external_location",
                str(loc["name"]),
                "--json",
                json.dumps({"changes": [{"principal": principal, "add": missing}]}),
            )
        if not loc.get("enable_file_events", True):
            acts.note(
                f"external location {loc['name']}: enable file events (owner action, Catalog > External locations)"
            )

    if cfg.env.alerts.get("enabled", True):
        wh = resolve_warehouse(ctx)
        usable, _ = check_warehouse(ctx) if wh else (False, "")
        if not wh:
            acts.note(f"warehouse '{cfg.env.alerts.get('warehouse_name')}' not found; set alerts.warehouse_name")
        elif not usable:  # never touch a shared warehouse's ACL when the deployer can already use it
            acts.do(
                "permissions",
                "update",
                "warehouses",
                str(wh["id"]),
                "--json",
                json.dumps(
                    {"access_control_list": [{"service_principal_name": principal, "permission_level": "CAN_USE"}]}
                ),
            )


def _tag_policies(cfg: ResolvedConfig, cli: Cli, acts: _Actions) -> None:
    if not cfg.governed_tags_enabled:
        acts.note("governed_tags is false: skipping tag policies")
        return
    for key, values in governed_tag_values(cfg).items():
        try:
            policy = cli.json("tag-policies", "get-tag-policy", key)
        except CliError:
            acts.do(
                "tag-policies",
                "create-tag-policy",
                "--json",
                json.dumps(
                    {
                        "tag_key": key,
                        "description": "Ingestion framework traceability tag",
                        "values": [{"name": v} for v in values],
                    }
                ),
            )
            continue
        have = [str(v.get("name")) for v in policy.get("values") or []]
        missing = [v for v in values if v not in have]
        if missing:
            acts.do(
                "tag-policies",
                "update-tag-policy",
                key,
                "values",
                "--json",
                json.dumps({"values": [{"name": v} for v in [*have, *missing]]}),
            )


ASSIGNER_ROLE = "roles/tagPolicy.assigner"
RULE_SETS = "/api/2.0/preview/accounts/access-control/rule-sets"


def _tag_assigner(cfg: ResolvedConfig, cli: Cli, acts: _Actions, principal: str) -> bool:
    """Let the deployer apply the governed tags (ASSIGN). Returns False when it cannot be automated."""
    account_id = str(cfg.env.governance.get("account_id") or "")
    if not cfg.governed_tags_enabled or not account_id or PLACEHOLDER.search(account_id):
        return False
    member = f"servicePrincipals/{principal}"
    for key in governed_tag_values(cfg):
        try:
            policy_id = str(cli.json("tag-policies", "get-tag-policy", key)["id"])
        except CliError:
            acts.note(f"tag policy {key} not created yet: re-run make prereqs to grant {principal} ASSIGN on it")
            continue
        name = f"accounts/{account_id}/tagPolicies/{policy_id}/ruleSets/default"
        rule_set = cli.json("api", "get", f"{RULE_SETS}?name={name}&etag=")
        rules = list(rule_set.get("grant_rules") or [])
        if any(r.get("role") == ASSIGNER_ROLE and member in (r.get("principals") or []) for r in rules):
            continue
        rules.append({"principals": [member], "role": ASSIGNER_ROLE})
        body = {"name": name, "rule_set": {"name": name, "etag": rule_set.get("etag", ""), "grant_rules": rules}}
        acts.do("api", "put", RULE_SETS, "--json", json.dumps(body))
    return True


def apply_prereqs(cfg: ResolvedConfig, cli: Cli, *, dry_run: bool = False, principal: str | None = None) -> list[str]:
    acts = _Actions(cli, dry_run)
    principal = principal or cfg.env.deployer_service_principal
    if principal and PLACEHOLDER.search(principal):
        principal = ""
    _storage(cfg, cli, acts)
    if principal:
        _grants(cfg, cli, acts, principal)
    else:
        acts.note("deployer.service_principal not set: skipping grants (you will deploy as yourself)")
    _tag_policies(cfg, cli, acts)
    manual = list(MANUAL_STEPS)
    if principal and _tag_assigner(cfg, cli, acts, principal):
        manual = [m for m in manual if not m.startswith("Deployer SP: grant it ASSIGN")]
    if cfg.env.seed_enabled:  # sandbox simulates Qlik output; there is no endpoint to configure
        manual = [m for m in manual if not m.startswith("Qlik Replicate:")]
    if cfg.env.governance.get("nonprod_data_attested"):
        manual = [m for m in manual if not m.startswith("Data governance:")]
    for step in manual:
        acts.note(f"MANUAL: {step}")
    return acts.log
