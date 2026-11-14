from __future__ import annotations

from ingestion_framework.bundle import generate
from ingestion_framework.config import load_config


def _spec(mini_repo, env="sandbox", **kw):
    return generate(load_config(mini_repo(env=env, **kw), env), git_commit="abc1234")


def test_one_pipeline_per_group_with_stable_keys_and_names(mini_repo):
    spec = _spec(mini_repo, groups={"g1": ["TBL_A"], "g2": ["TBL_B"]})

    assert sorted(spec["pipelines"]) == ["pipeline_testsrc_g1", "pipeline_testsrc_g2"]
    p = spec["pipelines"]["pipeline_testsrc_g1"]
    assert p["name"] == "ingest-testsrc-g1-sandbox"
    assert p["configuration"]["framework.group"] == "g1"
    assert p["libraries"] == [{"file": {"path": "${workspace.file_path}/src/pipelines/entrypoint.py"}}]
    assert p["event_log"] == {
        "catalog": "bronze_sandbox",
        "schema": "ingest_ops",
        "name": "event_log_testsrc_g1",
    }


def test_pipeline_tags_point_to_config_and_code(mini_repo):
    spec = _spec(mini_repo, env_overrides={"governance": {"tag_prefix": "x_"}})
    tags = spec["pipelines"]["pipeline_testsrc_g1"]["tags"]

    assert tags == {
        "x_ingest_pipeline": "ingest-testsrc-g1-sandbox",
        "x_ingest_source": "testsrc",
        "x_ingest_config": "sources/testsrc/groups/g1/",
        "x_ingest_code": "src/pipelines/entrypoint.py -> src/ingestion_framework/adapters/qlik_cdc_files",
        "x_ingest_git_commit": "abc1234",
    }


def test_notifications_cover_update_and_flow_failures(mini_repo):
    p = _spec(mini_repo)["pipelines"]["pipeline_testsrc_g1"]
    assert p["notifications"] == [
        {
            "email_recipients": ["ops@example.com"],
            "alerts": ["on-update-failure", "on-update-fatal-failure", "on-flow-failure"],
        }
    ]


def test_continuous_cadence_has_no_trigger_job(mini_repo):
    spec = _spec(mini_repo, env="prod", env_overrides={"cadence": {"default": "continuous"}})
    assert spec["pipelines"]["pipeline_testsrc_g1"]["continuous"] is True
    assert not [k for k in spec["jobs"] if k.startswith("job_testsrc")]


def test_file_arrival_cadence_creates_triggered_job_on_group_folder(mini_repo):
    spec = _spec(mini_repo)  # sandbox fixture default: on_arrival
    job = spec["jobs"]["job_testsrc_g1_trigger"]

    assert spec["pipelines"]["pipeline_testsrc_g1"]["continuous"] is False
    assert job["trigger"]["file_arrival"] == {
        "url": "s3://bucket/landing/testsrc/g1/",
        "min_time_between_triggers_seconds": 60,
        "wait_after_last_change_seconds": 60,
    }
    assert job["tasks"][0]["pipeline_task"] == {
        "pipeline_id": "${resources.pipelines.pipeline_testsrc_g1.id}",
        "full_refresh": False,
    }
    assert job["max_concurrent_runs"] == 1
    assert job["health"]["rules"] == [{"metric": "RUN_DURATION_SECONDS", "op": "GREATER_THAN", "value": 3600}]
    assert job["email_notifications"]["on_failure"] == ["ops@example.com"]


def test_weekly_sample_is_cron_with_lookback_passed_to_pipeline(mini_repo):
    spec = _spec(mini_repo, env="dev", env_overrides={"cadence": {"default": "weekly_sample"}})
    job = spec["jobs"]["job_testsrc_g1_trigger"]
    pipe = spec["pipelines"]["pipeline_testsrc_g1"]

    assert job["schedule"]["quartz_cron_expression"] == "0 0 6 ? * MON"
    assert "trigger" not in job
    assert pipe["configuration"]["framework.ct_lookback_days"] == "1"


def test_schemas_dqm_and_alerts_per_source_and_env(mini_repo):
    spec = _spec(mini_repo)

    assert sorted(spec["schemas"]) == ["schema_bronze_testsrc", "schema_ops", "schema_silver_testsrc"]
    assert spec["schemas"]["schema_silver_testsrc"] == {
        "catalog_name": "silver_sandbox",
        "name": "testsrc",
        "comment": "testsrc silver - declared and owned by the ingestion framework",
    }
    dqm = spec["data_quality_monitors"]["dqm_silver_testsrc"]
    assert dqm["object_type"] == "schema"
    assert dqm["object_id"] == "${resources.schemas.schema_silver_testsrc.schema_id}"
    # anomaly detection only accepts workspace user ids / destination ids (emails are resolved at deploy time)
    # No recipients -> no notification_settings at all (no emails). The config stays non-empty with a
    # never-existing excluded table, because CLI 1.26 drops an empty config and the API rejects the update.
    assert dqm["anomaly_detection_config"] == {
        "excluded_table_full_names": ["silver_sandbox.testsrc.__dqm_no_recipients"]
    }
    with_ids = generate(load_config(mini_repo(), "sandbox"), git_commit="x", dqm_user_ids=["42"])
    assert with_ids["data_quality_monitors"]["dqm_silver_testsrc"]["anomaly_detection_config"] == {
        "notification_settings": {"on_unhealthy_table": {"destination_user_ids": ["42"]}}
    }
    assert sorted(spec["alerts"]) == [
        "alert_expectation_failures",
        "alert_pipeline_health",
        "alert_tag_drift",
        "alert_undeclared_objects",
    ]
    alert = spec["alerts"]["alert_undeclared_objects"]
    assert alert["warehouse_id"] == "${var.warehouse_id}"
    assert "bronze_sandbox.ingest_ops.table_registry" in alert["query_text"]
    assert alert["evaluation"]["notification"]["subscriptions"] == [{"user_email": "ops@example.com"}]


def test_prod_guards(mini_repo):
    spec = _spec(
        mini_repo,
        env="prod",
        env_overrides={"pipelines": {"prevent_destroy": True}, "cadence": {"default": "continuous"}},
    )
    assert spec["pipelines"]["pipeline_testsrc_g1"]["lifecycle"] == {"prevent_destroy": True}
    assert all(s.get("lifecycle") == {"prevent_destroy": True} for s in spec["schemas"].values())


def test_governance_job_always_present_and_seed_only_when_enabled(mini_repo):
    spec = _spec(mini_repo)
    assert "job_governance" in spec["jobs"]
    assert "job_seed" not in spec["jobs"]
    seeded = _spec(mini_repo, env_overrides={"seed": {"enabled": True}})
    assert "job_seed" in seeded["jobs"]


def test_permissions_from_governance_groups(mini_repo):
    spec = _spec(mini_repo, env_overrides={"governance": {"admin_groups": ["admins"], "reader_groups": ["ops"]}})
    assert spec["pipelines"]["pipeline_testsrc_g1"]["permissions"] == [
        {"level": "CAN_MANAGE", "group_name": "admins"},
        {"level": "CAN_VIEW", "group_name": "ops"},
    ]
    assert {"principal": "ops", "privileges": ["USE_SCHEMA", "SELECT"]} in spec["schemas"]["schema_silver_testsrc"][
        "grants"
    ]


def test_undeclared_alert_ignores_sdp_internal_tables_like_the_audit(mini_repo):
    """SDP keeps `__materialization_*` backing tables in the target schema; the alert must skip them
    exactly like governance.job.undeclared_tables does, or it fires on every pipeline."""
    from ingestion_framework.governance.job import undeclared_tables

    query = _spec(mini_repo)["alerts"]["alert_undeclared_objects"]["query_text"]
    # Not `NOT startswith(...)`: pushed down into information_schema it silently matches nothing (seen live).
    assert "left(t.table_name, 2) <> '__'" in query and "startswith" not in query
    assert undeclared_tables({"c.s.__materialization_mat_x"}, set()) == []
