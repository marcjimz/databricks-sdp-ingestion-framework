from __future__ import annotations

import json

import pytest

from ingestion_framework.config import ConfigError, load_config
from ingestion_framework.ops import reload, start

SUMMARY = {
    "resources": {
        "pipelines": {"pipeline_testsrc_g1": {"id": "p1"}, "pipeline_testsrc_g2": {"id": "p2"}},
        "jobs": {"job_testsrc_g2_trigger": {"id": "j2"}},
    }
}


class RecordingCli:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def run(self, *args: str) -> None:
        self.calls.append(args)

    def json(self, *args: str):
        self.calls.append(args)
        if args[:2] == ("warehouses", "list"):
            return [{"name": "wh", "id": "w1"}]
        if args[:2] == ("api", "post"):
            return {"status": {"state": "SUCCEEDED"}}
        raise AssertionError(args)

    def text(self, *args: str) -> str:
        raise AssertionError(args)


def _cfg(mini_repo):
    root = mini_repo(
        env="prod",
        groups={"g1": ["TBL_A"], "g2": ["TBL_B"]},
        env_overrides={"cadence": {"default": "continuous", "overrides": {"testsrc/g2": "on_arrival"}}},
    )
    return load_config(root, "prod")


def test_start_runs_continuous_pipelines_and_drains_triggered_groups(mini_repo):
    cli = RecordingCli()
    lines = start(_cfg(mini_repo), cli, SUMMARY)
    assert ("pipelines", "start-update", "p1") in cli.calls
    assert ("jobs", "run-now", "j2", "--no-wait") in cli.calls
    assert len(lines) == 2


def test_reload_records_cutoff_and_refreshes_only_silver(mini_repo):
    cli = RecordingCli()
    reload(_cfg(mini_repo), cli, SUMMARY, table="TBL_A", since="20261113100000")
    statement = json.loads(next(c for c in cli.calls if c[:2] == ("api", "post"))[-1])["statement"]
    assert "reload_cutoffs" in statement and "20261113100000" in statement
    update = next(c for c in cli.calls if c[:2] == ("pipelines", "start-update"))
    assert update[2] == "p1"
    assert json.loads(update[-1]) == {"full_refresh_selection": ["silver_prod.testsrc.tbl_a"]}


def test_reload_rejects_bad_cutoffs_and_unknown_tables(mini_repo):
    cfg = _cfg(mini_repo)
    with pytest.raises(ConfigError, match="yyyyMMddHHmmss"):
        reload(cfg, RecordingCli(), SUMMARY, table="TBL_A", since="yesterday")
    with pytest.raises(ConfigError, match="exactly one"):
        reload(cfg, RecordingCli(), SUMMARY, table="NOPE", since="20261113100000")
