from __future__ import annotations

import json
from typing import Any

import pytest

from ingestion_framework.config import load_config
from ingestion_framework.prereqs import CliError, apply_prereqs, bundle_vars, run_preflight

SP = "sp-app-id"


class FakeCli:
    """Answers `databricks ... -o json` calls from an in-memory workspace description."""

    def __init__(self, **overrides: Any) -> None:
        self.state: dict[str, Any] = {
            "version": "Databricks CLI v1.26.0",
            "host": "https://ws.example.com",
            "me": {"userName": SP},
            "catalogs": {"bronze_sandbox": {"owner": "admin"}, "silver_sandbox": {"owner": "admin"}},
            "catalog_grants": {SP: ["USE_CATALOG", "CREATE_SCHEMA"]},
            "external_locations": [
                {
                    "name": "landing",
                    "url": "s3://bucket/landing/",
                    "enable_file_events": True,
                    "owner": "admin",
                }
            ],
            "location_grants": {SP: ["READ_FILES"]},
            "warehouses": [{"name": "wh", "id": "w1"}],
            "warehouse_acl": [{"group_name": "users", "all_permissions": [{"permission_level": "CAN_USE"}]}],
            "tag_policies": {
                "ingest_pipeline": ["ingest-testsrc-g1-sandbox"],
                "ingest_source": ["testsrc"],
                "ingest_layer": ["bronze", "silver"],
            },
        }
        self.state.update(overrides)
        self.calls: list[list[str]] = []

    def text(self, *args: str) -> str:
        self.calls.append(list(args))
        if args[:1] == ("version",):
            return self.state["version"]
        raise CliError(" ".join(args))

    def json(self, *args: str) -> Any:  # noqa: PLR0911 (a dispatcher)
        self.calls.append(list(args))
        s = self.state
        match args:
            case ("auth", "describe"):
                return {"details": {"host": s["host"]}}
            case ("current-user", "me"):
                return s["me"]
            case ("catalogs", "get", name):
                if name not in s["catalogs"]:
                    raise CliError(f"Catalog '{name}' does not exist.")
                return {"name": name, **s["catalogs"][name]}
            case ("grants", "get-effective", "catalog", _name, "--principal", principal):
                return {
                    "privilege_assignments": [
                        {
                            "principal": principal,
                            "privileges": [{"privilege": p} for p in s["catalog_grants"].get(principal, [])],
                        }
                    ]
                }
            case ("grants", "get-effective", "external_location", _name, "--principal", principal):
                return {
                    "privilege_assignments": [
                        {
                            "principal": principal,
                            "privileges": [{"privilege": p} for p in s["location_grants"].get(principal, [])],
                        }
                    ]
                }
            case ("external-locations", "list"):
                return s["external_locations"]
            case ("warehouses", "list"):
                return s["warehouses"]
            case ("permissions", "get", "warehouses", _id):
                return {"access_control_list": s["warehouse_acl"]}
            case ("users", "list", "--filter", flt):
                email = flt.split('"')[1]
                return [] if email == "ghost@example.com" else [{"id": "u-" + email.split("@")[0], "userName": email}]
            case ("tag-policies", "get-tag-policy", key):
                if key not in s["tag_policies"]:
                    raise CliError(f"tag policy {key} not found")
                return {"tag_key": key, "values": [{"name": v} for v in s["tag_policies"][key]]}
        raise CliError(f"unexpected call: {args}")

    def run(self, *args: str) -> None:
        self.calls.append(["RUN", *args])


def _results(mini_repo, cli=None, **repo_kw):
    cfg = load_config(mini_repo(**repo_kw), "sandbox")
    return {r.id: r for r in run_preflight(cfg, cli or FakeCli())}


def test_all_green_when_workspace_matches_config(mini_repo):
    results = _results(mini_repo, env_overrides={"governance": {"nonprod_data_attested": True}})
    failing = {k: (r.status, r.detail) for k, r in results.items() if r.status in {"fail", "warn"}}
    assert failing == {}


def test_placeholders_fail_and_block(mini_repo):
    results = _results(mini_repo, env_overrides={"landing": {"testsrc": "s3://<your-bucket>/landing"}})
    assert results["env_placeholders"].status == "fail"
    assert "landing.testsrc" in results["env_placeholders"].detail
    assert results["env_placeholders"].blocking


def test_file_events_off_is_a_required_failure(mini_repo):
    cli = FakeCli(
        external_locations=[{"name": "landing", "url": "s3://bucket/", "enable_file_events": False, "owner": "admin"}]
    )
    results = _results(mini_repo, cli)
    assert results["landing_external_location"].status == "pass"
    assert results["file_events"].status == "fail" and results["file_events"].blocking


def test_missing_location_catalog_and_privileges(mini_repo):
    cli = FakeCli(external_locations=[], catalogs={"bronze_sandbox": {"owner": "admin"}}, catalog_grants={})
    results = _results(mini_repo, cli)
    assert results["landing_external_location"].status == "fail"
    assert results["catalogs"].status == "fail" and "silver_sandbox" in results["catalogs"].detail
    assert results["catalog_privileges"].status == "fail"


def test_owner_counts_as_privileged(mini_repo):
    cli = FakeCli(
        catalogs={"bronze_sandbox": {"owner": SP}, "silver_sandbox": {"owner": SP}},
        catalog_grants={},
    )
    assert _results(mini_repo, cli)["catalog_privileges"].status == "pass"


def test_wrong_workspace_and_wrong_identity(mini_repo):
    cli = FakeCli(me={"userName": "someone@example.com"})
    results = _results(mini_repo, cli, env_overrides={"workspace_host": "https://other.example.com"})
    assert results["workspace_host"].status == "fail"
    # sandbox: deployer identity is only recommended
    assert results["deployer_identity"].status == "warn"
    assert not results["deployer_identity"].blocking


def test_tag_policies_missing_values_are_reported(mini_repo):
    cli = FakeCli(tag_policies={"ingest_source": ["other"], "ingest_layer": ["bronze", "silver"]})
    r = _results(mini_repo, cli)["tag_policies"]
    assert r.status == "warn"  # recommended in sandbox (required elsewhere)
    assert "ingest_pipeline" in r.detail and "testsrc" in r.detail


def test_tools_version_gate(mini_repo):
    r = _results(mini_repo, FakeCli(version="Databricks CLI v1.19.0"))["local_tools"]
    assert r.status == "fail" and "1.24" in r.detail


UsersCli = FakeCli


def test_bundle_vars_resolve_warehouse_deployer_and_dqm_user_ids(mini_repo):
    cfg = load_config(mini_repo(), "sandbox")
    assert bundle_vars(cfg, UsersCli()) == {"warehouse_id": "w1", "deployer_sp": SP, "dqm_user_ids": "u-dq"}


def test_unresolvable_dqm_recipients_warn(mini_repo):
    cfg = load_config(mini_repo(env_overrides={"dqm": {"notify": {"emails": ["ghost@example.com"]}}}), "sandbox")
    results = {r.id: r for r in run_preflight(cfg, UsersCli())}
    assert results["alert_recipients"].status == "warn" and "ghost@example.com" in results["alert_recipients"].detail


def test_apply_plans_grants_and_tag_policies_in_dry_run(mini_repo):
    cfg = load_config(mini_repo(), "sandbox")
    cli = FakeCli(catalog_grants={}, location_grants={}, tag_policies={})
    actions = apply_prereqs(cfg, cli, dry_run=True)
    text = "\n".join(actions)

    assert "grants update catalog bronze_sandbox" in text and "CREATE_SCHEMA" in text
    assert "grants update external_location landing" in text and "READ_FILES" in text
    assert "tag-policies create-tag-policy" in text and "ingest-testsrc-g1-sandbox" in text
    assert not [c for c in cli.calls if c[0] == "RUN"], "dry run must not execute anything"


@pytest.mark.parametrize("env", ["dev", "tst", "prod"])
def test_identity_is_required_outside_sandbox(mini_repo, env):
    cfg = load_config(mini_repo(env=env), env)
    results = {r.id: r for r in run_preflight(cfg, FakeCli(me={"userName": "human@example.com"}))}
    assert results["deployer_identity"].status == "fail" and results["deployer_identity"].blocking


def test_file_events_and_privileges_never_pass_without_a_covering_location(mini_repo):
    results = _results(mini_repo, FakeCli(external_locations=[]))
    assert results["file_events"].status == "fail"
    assert results["landing_privileges"].status == "fail"


AZURE_STORAGE = {
    "create": True,
    "cloud": "azure",
    "credential_name": "cred",
    "external_location_name": "landing",
    "access_connector_id": "/subscriptions/s1/resourceGroups/rg/providers/Microsoft.Databricks/accessConnectors/ac",
    "resource_group": "rg",
    "subscription_id": "s1",
    "url": "abfss://landing@acct.dfs.core.windows.net/",
    "catalog_storage_root": "abfss://catalogs@acct.dfs.core.windows.net/",
    "catalog_external_location_name": "catalogs",
}


def test_apply_creates_azure_storage_with_automatic_file_events(mini_repo):
    cfg = load_config(
        mini_repo(
            env_overrides={
                "storage": AZURE_STORAGE,
                "landing": {"testsrc": "abfss://landing@acct.dfs.core.windows.net/x"},
            }
        ),
        "sandbox",
    )
    cli = FakeCli(external_locations=[], catalogs={})
    cli.json = _missing_storage(cli.json)  # credential + location do not exist yet
    text = "\n".join(apply_prereqs(cfg, cli, dry_run=True))

    assert '"azure_managed_identity": {"access_connector_id": "/subscriptions/s1/' in text
    assert '"enable_file_events": true' in text
    assert '"managed_aqs": {"resource_group": "rg", "subscription_id": "s1"}' in text
    assert (
        "catalogs create bronze_sandbox --storage-root abfss://catalogs@acct.dfs.core.windows.net/bronze_sandbox"
        in text
    )
    # catalog storage needs its own external location (file events off: not an ingestion path)
    assert '"name": "catalogs", "url": "abfss://catalogs@acct.dfs.core.windows.net/"' in text
    # new locations enable file events by default; the catalog location must not (nobody reads that queue)
    catalog_location = next(line for line in text.splitlines() if '"name": "catalogs"' in line)
    assert '"enable_file_events": false' in catalog_location
    assert text.index('"name": "catalogs"') < text.index("catalogs create bronze_sandbox")


def test_apply_skips_warehouse_grant_when_already_usable(mini_repo):
    cfg = load_config(mini_repo(), "sandbox")
    text = "\n".join(apply_prereqs(cfg, FakeCli(), dry_run=True))  # FakeCli: `users` has CAN_USE
    assert "permissions update warehouses" not in text
    grant = "\n".join(apply_prereqs(cfg, FakeCli(warehouse_acl=[]), dry_run=True))
    assert "permissions update warehouses w1" in grant


def test_apply_refuses_unknown_storage_cloud(mini_repo):
    cfg = load_config(mini_repo(env_overrides={"storage": {**AZURE_STORAGE, "cloud": "gcp"}}), "sandbox")
    cli = FakeCli(external_locations=[], catalogs={})
    cli.json = _missing_storage(cli.json)
    assert any("storage.cloud" in line for line in apply_prereqs(cfg, cli, dry_run=True))


def _missing_storage(json_fn):
    def wrapped(*args):
        if args[:2] in {("storage-credentials", "get"), ("external-locations", "get")}:
            raise CliError("does not exist")
        return json_fn(*args)

    return wrapped


class RuleSetCli(FakeCli):
    """FakeCli + the account access-control rule-set API (via `databricks api`)."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.rule_sets = {}

    def json(self, *args):
        if args[:2] == ("tag-policies", "get-tag-policy") and args[2] in self.state["tag_policies"]:
            return {
                "id": f"id-{args[2]}",
                "tag_key": args[2],
                "values": [{"name": v} for v in self.state["tag_policies"][args[2]]],
            }
        if args[:2] == ("api", "get") and "rule-sets" in args[2]:
            name = args[2].split("name=")[1].split("&")[0]
            return self.rule_sets.get(name, {"name": name, "etag": "e1", "grant_rules": []})
        return super().json(*args)

    def run(self, *args):
        super().run(*args)
        if args[:2] == ("api", "put"):
            body = json.loads(args[-1])
            self.rule_sets[body["name"]] = body["rule_set"]


def test_apply_grants_deployer_tag_assigner_on_every_governed_policy(mini_repo):
    cfg = load_config(mini_repo(env_overrides={"governance": {"account_id": "acct-1"}}), "sandbox")
    cli = RuleSetCli()
    log = apply_prereqs(cfg, cli)  # real (fake) run: policies exist, no assigner yet
    puts = [c for c in cli.calls if c[:3] == ["RUN", "api", "put"]]

    assert len(puts) == 3
    body = json.loads(puts[0][-1])
    assert body["name"].startswith("accounts/acct-1/tagPolicies/id-ingest_")
    assert {"principals": [f"servicePrincipals/{SP}"], "role": "roles/tagPolicy.assigner"} in body["rule_set"][
        "grant_rules"
    ]
    assert not any("MANUAL: Deployer SP: grant it ASSIGN" in line for line in log)
    # idempotent: a second run changes nothing
    cli.calls.clear()
    apply_prereqs(cfg, cli)
    assert not [c for c in cli.calls if c[:3] == ["RUN", "api", "put"]]


class NoManageCli(FakeCli):
    """A deployer that can USE the warehouse but cannot read its ACL (no Manage)."""

    def __init__(self, statement_state="SUCCEEDED", **kw):
        super().__init__(**kw)
        self.statement_state = statement_state

    def json(self, *args):
        if args[:3] == ("permissions", "get", "warehouses"):
            raise CliError("does not have Manage permissions on SQL warehouse")
        if args[:2] == ("api", "post") and args[2] == "/api/2.0/sql/statements":
            return {"status": {"state": self.statement_state}}
        return super().json(*args)


def test_warehouse_check_falls_back_to_a_probe_query_without_manage(mini_repo):
    assert _results(mini_repo, NoManageCli())["warehouse"].status == "pass"
    denied = _results(mini_repo, NoManageCli(statement_state="FAILED"))["warehouse"]
    assert denied.status == "fail" and "cannot run" in denied.detail
