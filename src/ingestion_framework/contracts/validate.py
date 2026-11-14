"""Validate contracts against the vendored ODCS v3.2.0 JSON schema and the house profile.

Used by lint, pre-commit and tests (requires ``jsonschema``, a dev dependency); never imported
by pipeline code.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any

ODCS_VERSION = "v3.2.0"
SCHEMA_PATH = Path(__file__).resolve().parents[3] / "contracts" / f"odcs-json-schema-{ODCS_VERSION}.json"

# Beyond the ODCS minimum (apiVersion, kind, id, version) every operational contract must say
# who owns it, its lifecycle status, its SLA and its data classification.
HOUSE_REQUIRED_TOP_LEVEL = ("status", "team", "slaProperties")
CLASSIFICATION_PROPERTY = "dataClassification"


@lru_cache(maxsize=1)
def _validator() -> Any:
    from jsonschema import Draft201909Validator

    schema = json.loads(SCHEMA_PATH.read_text())
    return Draft201909Validator(schema)


def schema_problems(doc: Mapping[str, Any]) -> list[str]:
    problems = []
    for error in sorted(_validator().iter_errors(doc), key=lambda e: list(e.absolute_path)):
        where = "/".join(str(p) for p in error.absolute_path) or "<root>"
        problems.append(f"{where}: {error.message}")
    return problems


def profile_problems(doc: Mapping[str, Any]) -> list[str]:
    problems: list[str] = []
    if doc.get("apiVersion") != ODCS_VERSION:
        problems.append(f"apiVersion must be {ODCS_VERSION}, got {doc.get('apiVersion')!r}")
    for key in HOUSE_REQUIRED_TOP_LEVEL:
        if not doc.get(key):
            problems.append(f"house profile: '{key}' is required")
    props = doc.get("customProperties") or []
    if not any(isinstance(p, Mapping) and p.get("property") == CLASSIFICATION_PROPERTY for p in props):
        problems.append(f"house profile: customProperties.{CLASSIFICATION_PROPERTY} is required")
    return problems
