from __future__ import annotations

import pytest
import yaml

from ingestion_framework.bundle import generate
from ingestion_framework.config import ConfigError, load_config
from ingestion_framework.config.fingerprint import assert_fingerprint, config_fingerprint


def test_fingerprint_changes_with_local_overrides(mini_repo):
    root = mini_repo()
    before = config_fingerprint(load_config(root, "sandbox"))
    (root / "conf/environments/sandbox.local.yml").write_text(yaml.safe_dump({"landing": {"testsrc": "s3://x/y"}}))
    after = config_fingerprint(load_config(root, "sandbox"))
    assert before != after and len(after) == 16


def test_generated_pipelines_and_jobs_carry_the_deploy_time_fingerprint(mini_repo):
    cfg = load_config(mini_repo(env_overrides={"seed": {"enabled": True}}), "sandbox")
    spec = generate(cfg, git_commit="x")
    fp = config_fingerprint(cfg)
    assert spec["pipelines"]["pipeline_testsrc_g1"]["configuration"]["framework.config_fingerprint"] == fp
    for key in ("job_governance", "job_seed"):
        params = spec["jobs"][key]["tasks"][0]["spark_python_task"]["parameters"]
        assert params[params.index("--config-fingerprint") + 1] == fp


def test_runtime_refuses_a_config_that_differs_from_deploy_time(mini_repo):
    cfg = load_config(mini_repo(), "sandbox")
    assert_fingerprint(cfg, config_fingerprint(cfg))  # same config: fine
    assert_fingerprint(cfg, "")  # not provided (older deploy): skipped
    with pytest.raises(ConfigError, match=r"local\.yml"):
        assert_fingerprint(cfg, "deadbeefdeadbeef")
