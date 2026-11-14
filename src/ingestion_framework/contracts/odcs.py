"""Read ODCS v3.2.0 data contracts into the framework's Contract model.

Parsing is deliberately lenient (it never needs jsonschema, so it runs inside pipelines);
structural validation against the vendored JSON schema lives in :mod:`.validate` and runs
in lint / tests / pre-commit.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ingestion_framework.config.models import Column, ConfigError, Contract, Expectation

_SAFE_NAME = re.compile(r"[^a-zA-Z0-9_]+")


def _custom_property(items: Any, name: str) -> Any:
    for item in items or []:
        if isinstance(item, Mapping) and item.get("property") == name:
            return item.get("value")
    return None


def _expectation_name(raw: str) -> str:
    return _SAFE_NAME.sub("_", raw).strip("_").lower()


def rule_to_expectation(
    rule: Mapping[str, Any],
    *,
    policy: Mapping[str, Any],
    where: str,
    column: str | None = None,
) -> Expectation | None:
    """Map one ODCS quality rule to an SDP expectation.

    Only ``type: custom`` rules whose engine is the framework's SDP engine become
    expectations. ``text`` rules are documentation. Everything else is rejected by lint
    (see :func:`ingestion_framework.lint`), so returning ``None`` here is never silent.
    """
    if rule.get("type") != "custom" or rule.get("engine") != policy.get("sdp_engine"):
        return None
    implementation = rule.get("implementation")
    if not isinstance(implementation, str) or not implementation.strip():
        raise ConfigError(f"{where}: custom rule needs a SQL boolean string 'implementation'")
    if column:
        implementation = implementation.replace("${column}", f"`{column}`")
    action = str(_custom_property(rule.get("customProperties"), "action") or policy["default_action"])
    name = str(rule.get("name") or implementation)
    return Expectation(name=_expectation_name(name), condition=implementation.strip(), action=action)


def parse_contract(doc: Mapping[str, Any], path: Path, policy: Mapping[str, Any]) -> Contract:
    if not isinstance(doc, Mapping):
        raise ConfigError(f"{path}: contract must be a YAML mapping")
    objects = doc.get("schema") or []
    if len(objects) != 1:
        raise ConfigError(f"{path}: expected exactly one schema object per table contract, got {len(objects)}")
    obj = objects[0]

    columns: list[Column] = []
    expectations: list[Expectation] = []
    for prop in obj.get("properties") or []:
        name = str(prop["name"])
        pk_pos = prop.get("primaryKeyPosition")
        if prop.get("primaryKey") and pk_pos is None:
            pk_pos = 1 + sum(1 for c in columns if c.primary_key_position is not None)
        required = bool(prop.get("required", False)) or bool(prop.get("primaryKey", False))
        columns.append(
            Column(
                name=name,
                logical_type=str(prop.get("logicalType", "string")),
                physical_type=prop.get("physicalType"),
                required=required,
                primary_key_position=int(pk_pos) if prop.get("primaryKey") else None,
                description=prop.get("description"),
                examples=tuple(str(x) for x in prop.get("examples") or ()),
            )
        )
        if required:
            expectations.append(
                Expectation(
                    name=_expectation_name(f"{name}_not_null"),
                    condition=f"`{name}` IS NOT NULL",
                    action=str(policy["default_action"]),
                )
            )
        for rule in prop.get("quality") or []:
            exp = rule_to_expectation(rule, policy=policy, where=f"{path}:{name}", column=name)
            if exp:
                expectations.append(exp)

    for rule in obj.get("quality") or []:
        exp = rule_to_expectation(rule, policy=policy, where=str(path))
        if exp:
            expectations.append(exp)

    names = [e.name for e in expectations]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ConfigError(f"{path}: duplicate expectation names {duplicates}")

    description = obj.get("description")
    return Contract(
        path=path,
        id=str(doc.get("id", "")),
        version=str(doc.get("version", "")),
        status=str(doc.get("status", "")),
        object_name=str(obj.get("name", "")),
        physical_name=str(obj.get("physicalName") or obj.get("name", "")),
        description=str(description) if description else None,
        columns=tuple(columns),
        expectations=tuple(expectations),
        classification=_custom_property(doc.get("customProperties"), "dataClassification"),
        raw=doc,
    )


def unsupported_rules(doc: Mapping[str, Any], policy: Mapping[str, Any]) -> list[str]:
    """Quality rules the framework would not enforce (lint turns these into errors)."""
    problems: list[str] = []
    engine = policy.get("sdp_engine")

    def check(rule: Mapping[str, Any], where: str) -> None:
        kind = rule.get("type", "library")
        if kind == "text":
            return
        if kind == "custom" and rule.get("engine") == engine:
            action = _custom_property(rule.get("customProperties"), "action")
            if action is not None and action not in policy.get("allowed_actions", []):
                problems.append(f"{where}: action '{action}' not allowed (use {policy['allowed_actions']})")
            return
        problems.append(
            f"{where}: rule type '{kind}'"
            + (f" engine '{rule.get('engine')}'" if rule.get("engine") else "")
            + f" is not enforced by the framework (use type: custom, engine: {engine})"
        )

    for obj in doc.get("schema") or []:
        for rule in obj.get("quality") or []:
            check(rule, f"schema[{obj.get('name')}]")
        for prop in obj.get("properties") or []:
            for rule in prop.get("quality") or []:
                check(rule, f"schema[{obj.get('name')}].{prop.get('name')}")
    return problems
