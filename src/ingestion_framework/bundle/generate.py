"""Generate every bundle resource for one environment as plain dicts (bundle schema shapes).

Pure and deterministic, so the whole deployment is unit-testable; ``resources/__init__.py``
converts the dicts into ``databricks.bundles`` objects at deploy time.

Cross-cutting governance is applied here, around each adapter's ``pipeline_body``, so every
source type gets the same naming, tags, event log, notifications, cadence, permissions and
protection without re-implementing them.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ingestion_framework.adapters import get_adapter
from ingestion_framework.adapters.base import WORKSPACE_FILE_PATH
from ingestion_framework.bundle.alerts import build_alerts
from ingestion_framework.config import CadenceProfile, GroupConfig, ResolvedConfig
from ingestion_framework.config.fingerprint import config_fingerprint
from ingestion_framework.governance.tags import pipeline_tags

PIPELINE_ALERTS = ["on-update-failure", "on-update-fatal-failure", "on-flow-failure"]
JOB_ENV = [{"environment_key": "default", "spec": {"environment_version": "4", "dependencies": ["pyyaml>=6.0"]}}]

Spec = dict[str, dict[str, dict[str, Any]]]


def pipeline_key(source: str, group: str) -> str:
    return f"pipeline_{source}_{group}"


def _protect(cfg: ResolvedConfig, resource: dict[str, Any]) -> dict[str, Any]:
    if cfg.env.pipelines.get("prevent_destroy", False):
        resource["lifecycle"] = {"prevent_destroy": True}
    return resource


def _permissions(cfg: ResolvedConfig, *, read_level: str) -> list[dict[str, str]]:
    perms = [{"level": "CAN_MANAGE", "group_name": g} for g in cfg.env.governance.get("admin_groups") or []]
    perms += [{"level": read_level, "group_name": g} for g in cfg.env.governance.get("reader_groups") or []]
    return perms


def _with_permissions(cfg: ResolvedConfig, resource: dict[str, Any], *, read_level: str) -> dict[str, Any]:
    perms = _permissions(cfg, read_level=read_level)
    if perms:
        resource["permissions"] = perms
    return resource


# ---- schemas + DQM --------------------------------------------------------------------
def _schemas(cfg: ResolvedConfig) -> dict[str, dict[str, Any]]:
    readers = cfg.env.governance.get("reader_groups") or []
    out: dict[str, dict[str, Any]] = {}
    for src in cfg.sources_in_scope():
        for layer in ("bronze", "silver"):
            schema: dict[str, Any] = {
                "catalog_name": cfg.catalog(layer),
                "name": cfg.schema(src.source),
                "comment": f"{src.source} {layer} - declared and owned by the ingestion framework",
            }
            if readers:
                schema["grants"] = [{"principal": g, "privileges": ["USE_SCHEMA", "SELECT"]} for g in readers]
            out[f"schema_{layer}_{src.source}"] = _protect(cfg, schema)
    out["schema_ops"] = _protect(
        cfg,
        {
            "catalog_name": cfg.catalog("bronze"),
            "name": cfg.ops_schema,
            "comment": "Ingestion framework operations: event logs, table registry, audits",
        },
    )
    return out


def _dqm(cfg: ResolvedConfig, dqm_user_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    if not cfg.env.dqm.get("enabled", True):
        return {}
    # Anomaly-detection notifications accept workspace user ids and destination ids only; the configured
    # emails are resolved to user ids at deploy time (prereqs bundle-vars -> var.dqm_user_ids). Always
    # declared explicitly, even when empty: an omitted setting is not reconciled on redeploy.
    notify: dict[str, Any] = {}
    if dqm_user_ids:
        notify["destination_user_ids"] = list(dqm_user_ids)
    if cfg.env.dqm_destination_ids:
        notify["destination_ids"] = list(cfg.env.dqm_destination_ids)
    anomaly_base: dict[str, Any] = {"notification_settings": {"on_unhealthy_table": notify}} if notify else {}
    out: dict[str, dict[str, Any]] = {}
    for src in cfg.sources_in_scope():
        for layer in ("bronze", "silver"):
            anomaly = dict(anomaly_base) or {
                # CLI 1.26 drops an empty anomaly_detection_config, which the API rejects on update. With no
                # recipients, keep it non-empty with a table name that can never exist (`__` prefix).
                "excluded_table_full_names": [f"{cfg.catalog(layer)}.{cfg.schema(src.source)}.__dqm_no_recipients"]
            }
            out[f"dqm_{layer}_{src.source}"] = {
                "object_type": "schema",
                "object_id": f"${{resources.schemas.schema_{layer}_{src.source}.schema_id}}",
                "anomaly_detection_config": anomaly,
            }
    return out


# ---- pipelines + cadence jobs -----------------------------------------------------------
def _pipeline(cfg: ResolvedConfig, group: GroupConfig, cadence: CadenceProfile, git_commit: str) -> dict[str, Any]:
    adapter = get_adapter(cfg.source(group.source).type)
    pipeline: dict[str, Any] = {
        "name": cfg.pipeline_name(group.source, group.group),
        **adapter.pipeline_body(cfg, group),
        "serverless": bool(cfg.env.pipelines.get("serverless", True)),
        "channel": str(cfg.env.pipelines.get("channel", "CURRENT")),
        "continuous": cadence.mode == "continuous",
        "event_log": {
            "catalog": cfg.catalog("bronze"),
            "schema": cfg.ops_schema,
            "name": cfg.event_log_fqn(group.source, group.group).rsplit(".", 1)[-1],
        },
        "tags": pipeline_tags(cfg, group, adapter, git_commit=git_commit),
    }
    if "configuration" in pipeline:  # framework-code pipelines verify they run the deployed config
        pipeline["configuration"] = {
            **pipeline["configuration"],
            "framework.config_fingerprint": config_fingerprint(cfg),
        }
    if cfg.env.alert_emails:
        pipeline["notifications"] = [{"email_recipients": list(cfg.env.alert_emails), "alerts": PIPELINE_ALERTS}]
    _with_permissions(cfg, pipeline, read_level="CAN_VIEW")
    return _protect(cfg, pipeline)


def _trigger_job(cfg: ResolvedConfig, group: GroupConfig, cadence: CadenceProfile, git_commit: str) -> dict[str, Any]:
    adapter = get_adapter(cfg.source(group.source).type)
    s = cadence.settings
    job: dict[str, Any] = {
        "name": cfg.job_name(group.source, group.group),
        "description": f"{cadence.name} cadence for {cfg.pipeline_name(group.source, group.group)}",
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "tasks": [
            {
                "task_key": "pipeline_update",
                "pipeline_task": {
                    "pipeline_id": f"${{resources.pipelines.{pipeline_key(group.source, group.group)}.id}}",
                    "full_refresh": False,
                },
            }
        ],
        "tags": pipeline_tags(cfg, group, adapter, git_commit=git_commit),
    }
    if cadence.mode == "file_arrival":
        url = adapter.trigger_url(cfg, group)
        if not url:
            raise ValueError(f"{group.key}: adapter '{adapter.type}' has no file-arrival location")
        job["trigger"] = {
            "pause_status": "UNPAUSED",
            "file_arrival": {
                "url": url,
                "min_time_between_triggers_seconds": int(s["min_time_between_triggers_seconds"]),
                "wait_after_last_change_seconds": int(s["wait_after_last_change_seconds"]),
            },
        }
    elif cadence.mode == "periodic":
        job["trigger"] = {
            "pause_status": "UNPAUSED",
            "periodic": {"interval": int(s["interval"]), "unit": str(s["unit"])},
        }
    elif cadence.mode == "cron":
        job["schedule"] = {
            "quartz_cron_expression": str(s["quartz_cron_expression"]),
            "timezone_id": str(s.get("timezone_id", "UTC")),
            "pause_status": "UNPAUSED",
        }
    if s.get("max_run_duration_seconds"):
        job["health"] = {
            "rules": [
                {
                    "metric": "RUN_DURATION_SECONDS",
                    "op": "GREATER_THAN",
                    "value": int(s["max_run_duration_seconds"]),
                }
            ]
        }
    if cfg.env.alert_emails:
        emails = list(cfg.env.alert_emails)
        job["email_notifications"] = {"on_failure": emails, "on_duration_warning_threshold_exceeded": emails}
    return _with_permissions(cfg, job, read_level="CAN_VIEW")


def _python_job(cfg: ResolvedConfig, *, name: str, script: str, args: list[str], description: str) -> dict[str, Any]:
    params = [
        "--root",
        WORKSPACE_FILE_PATH,
        "--env",
        cfg.env.name,
        "--config-fingerprint",
        config_fingerprint(cfg),
        *args,
    ]
    job: dict[str, Any] = {
        "name": name,
        "description": description,
        "max_concurrent_runs": 1,
        "environments": JOB_ENV,
        "tasks": [
            {
                "task_key": "main",
                "environment_key": "default",
                "spark_python_task": {"python_file": f"{WORKSPACE_FILE_PATH}/{script}", "parameters": params},
            }
        ],
        "tags": {cfg.tag_key("ingest_code"): script},
    }
    if cfg.env.alert_emails:
        job["email_notifications"] = {"on_failure": list(cfg.env.alert_emails)}
    return _with_permissions(cfg, job, read_level="CAN_VIEW")


def _governance_job(cfg: ResolvedConfig, git_commit: str) -> dict[str, Any]:
    gov = cfg.framework.section("governance_job")
    job = _python_job(
        cfg,
        name=f"ingest-governance-{cfg.env.name}",
        script="src/jobs/governance.py",
        args=["--git-commit", git_commit],
        description="Publishes the table registry, applies traceability tags, audits undeclared objects.",
    )
    job["schedule"] = {
        "quartz_cron_expression": str(gov.get("schedule", "0 30 * * * ?")),
        "timezone_id": str(gov.get("timezone", "UTC")),
        "pause_status": "UNPAUSED",
    }
    return job


def _seed_job(cfg: ResolvedConfig) -> dict[str, Any]:
    job = _python_job(
        cfg,
        name=f"ingest-seed-{cfg.env.name}",
        script="src/jobs/seed.py",
        args=["--batch", "{{job.parameters.batch}}", "--table", "{{job.parameters.table}}"],
        description="SANDBOX ONLY: writes synthetic Qlik Replicate files to the landing zone.",
    )
    job["parameters"] = [{"name": "batch", "default": "full_load"}, {"name": "table", "default": ""}]
    return job


# ---- alerts ---------------------------------------------------------------------------
def _alerts(cfg: ResolvedConfig) -> dict[str, dict[str, Any]]:
    if not cfg.env.alerts.get("enabled", True):
        return {}
    subscriptions = [{"user_email": e} for e in cfg.env.alert_emails]
    subscriptions += [{"destination_id": d} for d in cfg.env.alert_destination_ids]
    out: dict[str, dict[str, Any]] = {}
    for spec in build_alerts(cfg):
        alert: dict[str, Any] = {
            "display_name": f"[ingest][{cfg.env.name}] {spec.title}",
            "custom_description": spec.description,
            "query_text": spec.query,
            "warehouse_id": "${var.warehouse_id}",
            "evaluation": {
                "comparison_operator": "GREATER_THAN",
                "source": {"name": spec.column},
                "threshold": {"value": {"double_value": 0}},
                "empty_result_state": "OK",
                "notification": {"notify_on_ok": True, "subscriptions": subscriptions},
            },
            "schedule": {
                "quartz_cron_schedule": str(cfg.env.alerts.get("schedule", "0 0/15 * * * ?")),
                "timezone_id": str(cfg.env.alerts.get("timezone", "UTC")),
                "pause_status": "UNPAUSED",
            },
        }
        out[f"alert_{spec.name}"] = _with_permissions(cfg, alert, read_level="CAN_READ")
    return out


def generate(cfg: ResolvedConfig, *, git_commit: str, dqm_user_ids: Sequence[str] = ()) -> Spec:
    pipelines: dict[str, dict[str, Any]] = {}
    jobs: dict[str, dict[str, Any]] = {}
    for group in cfg.groups_in_scope():
        cadence = cfg.cadence_for(group.source, group.group)
        pipelines[pipeline_key(group.source, group.group)] = _pipeline(cfg, group, cadence, git_commit)
        if cadence.is_triggered:
            jobs[f"job_{group.source}_{group.group}_trigger"] = _trigger_job(cfg, group, cadence, git_commit)
    jobs["job_governance"] = _governance_job(cfg, git_commit)
    if cfg.env.seed_enabled:
        jobs["job_seed"] = _seed_job(cfg)
    return {
        "schemas": _schemas(cfg),
        "pipelines": pipelines,
        "jobs": jobs,
        "data_quality_monitors": _dqm(cfg, dqm_user_ids),
        "alerts": _alerts(cfg),
    }
