"""Guard that runtime loads the same environment config the bundle was generated from.

Resources are generated on the deploying machine from conf/environments/<env>.yml + <env>.local.yml;
pipelines and jobs reload the config from the deployed files. If the two differ (e.g. a gitignored
local override that was not synced), runtime silently uses other values — for example a placeholder
landing path, which surfaces as a misleading "permission on ANY FILE" error. The bundle passes a
fingerprint of the resolved environment to every pipeline and job; runtime refuses to start on mismatch.
"""

from __future__ import annotations

import hashlib
import json

from ingestion_framework.config.models import ConfigError
from ingestion_framework.config.resolved import ResolvedConfig


def config_fingerprint(cfg: ResolvedConfig) -> str:
    payload = json.dumps(cfg.env.raw, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def assert_fingerprint(cfg: ResolvedConfig, expected: str) -> None:
    if not expected:  # deployed before fingerprints existed
        return
    actual = config_fingerprint(cfg)
    if actual != expected:
        raise ConfigError(
            f"environment '{cfg.env.name}' config at runtime ({actual}) differs from the config the bundle was "
            f"deployed with ({expected}). Was conf/environments/{cfg.env.name}.local.yml synced? Redeploy with "
            f"`make deploy {cfg.env.name}` from the machine that holds the environment's values."
        )
