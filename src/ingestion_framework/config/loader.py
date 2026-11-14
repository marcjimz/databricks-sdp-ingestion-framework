"""Load the configuration tree: framework defaults → environment (+ local override) → sources."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from ingestion_framework.config.models import (
    ConfigError,
    EnvConfig,
    FrameworkConfig,
    GroupConfig,
    SourceConfig,
    TableConfig,
)
from ingestion_framework.config.resolved import ResolvedConfig

CONTRACT_SUFFIX = ".odcs.yaml"
GROUP_FILE = "_group.yml"


def read_yaml(path: Path) -> Any:
    try:
        return yaml.safe_load(path.read_text()) or {}
    except FileNotFoundError as exc:
        raise ConfigError(f"{path}: file not found") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path}: invalid YAML: {exc}") from exc


def deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def available_envs(root: Path) -> list[str]:
    env_dir = root / "conf" / "environments"
    return sorted(p.stem for p in env_dir.glob("*.yml") if not p.name.endswith(".local.yml"))


def load_env(root: Path, env: str) -> EnvConfig:
    path = root / "conf" / "environments" / f"{env}.yml"
    if not path.exists():
        raise ConfigError(f"no environment file {path} (available: {', '.join(available_envs(root)) or 'none'})")
    raw = read_yaml(path)
    local = path.with_name(f"{env}.local.yml")
    if local.exists():
        raw = deep_merge(raw, read_yaml(local))
    if raw.get("env") != env:
        raise ConfigError(f"{path}: 'env' must be '{env}', got {raw.get('env')!r}")
    return EnvConfig(name=env, short=str(raw.get("short") or env), raw=raw, path=path)


def _load_table(
    path: Path, source: str, group: str, defaults: Mapping[str, Any], policy: Mapping[str, Any]
) -> TableConfig:
    # Imported here: contracts depends on config.models, so a module-level import would cycle.
    from ingestion_framework.contracts.odcs import parse_contract

    raw = read_yaml(path)
    if "table" not in raw:
        raise ConfigError(f"{path}: missing required key 'table'")
    contract_path = path.with_name(path.name[: -len(".yml")] + CONTRACT_SUFFIX)
    if not contract_path.exists():
        raise ConfigError(f"{path}: every table needs a data contract; missing {contract_path.name}")
    contract = parse_contract(read_yaml(contract_path), contract_path, policy)
    table = str(raw["table"])
    return TableConfig(
        source=source,
        group=group,
        table=table,
        name=str(raw.get("name") or table.lower()),
        enabled=bool(raw.get("enabled", True)),
        scd_type=int(raw.get("scd_type", defaults.get("scd_type", 1))),
        silver=bool(raw.get("silver", defaults.get("silver", True))),
        append_only=bool(raw.get("append_only", False)),
        classification=str(
            raw.get("classification") or contract.classification or defaults.get("classification", "internal")
        ),
        config_path=path,
        contract_path=contract_path,
        contract=contract,
        raw=raw,
    )


def _load_group(gdir: Path, source: str, defaults: Mapping[str, Any], policy: Mapping[str, Any]) -> GroupConfig:
    gpath = gdir / GROUP_FILE
    raw = read_yaml(gpath)
    group = str(raw.get("group") or gdir.name)
    if group != gdir.name:
        raise ConfigError(f"{gpath}: 'group' ({group}) must match its folder name ({gdir.name})")
    tables = tuple(
        _load_table(p, source, group, defaults, policy) for p in sorted(gdir.glob("*.yml")) if p.name != GROUP_FILE
    )
    return GroupConfig(
        source=source,
        group=group,
        description=str(raw.get("description", "")),
        domain=str(raw.get("domain", group)),
        landing_folder=str(raw.get("landing_folder", group)),
        path=gdir,
        tables=tables,
    )


def load_source(sdir: Path, policy: Mapping[str, Any]) -> SourceConfig:
    spath = sdir / "source.yml"
    raw = read_yaml(spath)
    source = str(raw.get("source") or sdir.name)
    if source != sdir.name:
        raise ConfigError(f"{spath}: 'source' ({source}) must match its folder name ({sdir.name})")
    if "type" not in raw:
        raise ConfigError(f"{spath}: missing required key 'type' (the source adapter)")
    defaults = raw.get("defaults") or {}
    groups = tuple(
        _load_group(g, source, defaults, policy)
        for g in sorted((sdir / "groups").iterdir())
        if g.is_dir() and (g / GROUP_FILE).exists()
    )
    settings = {k: v for k, v in raw.items() if k not in {"source", "type", "description", "defaults"}}
    return SourceConfig(
        source=source,
        type=str(raw["type"]),
        description=str(raw.get("description", "")),
        settings=settings,
        defaults=defaults,
        path=sdir,
        groups=groups,
    )


def load_config(root: Path | str, env: str) -> ResolvedConfig:
    root = Path(root)
    framework = FrameworkConfig(raw=read_yaml(root / "conf" / "framework.yml"))
    env_cfg = load_env(root, env)
    policy = framework.expectations
    sources_dir = root / "sources"
    sources = tuple(
        load_source(s, policy) for s in sorted(sources_dir.iterdir()) if s.is_dir() and (s / "source.yml").exists()
    )
    return ResolvedConfig(root=root, framework=framework, env=env_cfg, sources=sources)
