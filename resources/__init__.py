"""Bundle resource loader: every resource for the selected target comes from configuration.

Called by the Databricks CLI during `bundle validate/deploy` (see `python:` in databricks.yml).
All logic lives in ingestion_framework.bundle.generate (unit-tested); this only converts the
generated dicts into databricks.bundles objects.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from databricks.bundles.alerts import Alert
from databricks.bundles.core import Bundle, Resources
from databricks.bundles.data_quality_monitors import DataQualityMonitor
from databricks.bundles.jobs import Job
from databricks.bundles.pipelines import Pipeline
from databricks.bundles.schemas import Schema

from ingestion_framework.bundle import generate
from ingestion_framework.config import load_config

ROOT = Path(__file__).resolve().parents[1]


def _git_commit() -> str:
    def git(*args: str) -> str:
        return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=False).stdout.strip()

    sha = git("rev-parse", "--short", "HEAD")
    if not sha:
        return "uncommitted"
    return f"{sha}-dirty" if git("status", "--porcelain", "--untracked-files=no") else sha


def load_resources(bundle: Bundle) -> Resources:
    cfg = load_config(ROOT, bundle.target)
    dqm_user_ids = [i for i in str(bundle.variables.get("dqm_user_ids") or "").split(",") if i]
    spec = generate(cfg, git_commit=_git_commit(), dqm_user_ids=dqm_user_ids)
    resources = Resources()

    for key, schema in spec["schemas"].items():
        resources.add_schema(key, Schema.from_dict(schema))
    for key, pipeline in spec["pipelines"].items():
        resources.add_pipeline(key, Pipeline.from_dict(pipeline))
    for key, job in spec["jobs"].items():
        resources.add_job(key, Job.from_dict(job))
    for key, monitor in spec["data_quality_monitors"].items():
        resources.add_data_quality_monitor(key, DataQualityMonitor.from_dict(monitor))

    if spec["alerts"] and not bundle.variables.get("warehouse_id"):
        resources.add_diagnostic_warning(
            "warehouse_id is empty: health alerts were NOT generated. Deploy with `make deploy` "
            "(it resolves alerts.warehouse_name) or pass --var warehouse_id=<id>."
        )
    else:
        for key, alert in spec["alerts"].items():
            resources.add_alert(key, Alert.from_dict(alert))
    return resources
