"""Typed, immutable views over the YAML configuration tree."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class ConfigError(ValueError):
    """Raised for any invalid or missing configuration, always naming the offending file."""


@dataclass(frozen=True)
class Column:
    name: str
    logical_type: str
    physical_type: str | None
    required: bool
    primary_key_position: int | None
    description: str | None = None
    examples: tuple[str, ...] = ()  # ODCS `examples`; the synthetic generator samples from them


@dataclass(frozen=True)
class Expectation:
    name: str
    condition: str
    action: str  # warn | fail


@dataclass(frozen=True)
class Contract:
    path: Path
    id: str
    version: str
    status: str
    object_name: str
    physical_name: str
    description: str | None
    columns: tuple[Column, ...]
    expectations: tuple[Expectation, ...]
    classification: str | None
    raw: Mapping[str, Any] = field(repr=False, compare=False)

    @property
    def keys(self) -> tuple[str, ...]:
        keyed = [c for c in self.columns if c.primary_key_position is not None]
        return tuple(c.name for c in sorted(keyed, key=lambda c: c.primary_key_position or 0))


@dataclass(frozen=True)
class TableConfig:
    source: str
    group: str
    table: str  # source (physical) name, e.g. CMC_MEME_MEMBER — also the Qlik folder name
    name: str  # target table name in UC, e.g. cmc_meme_member
    enabled: bool
    scd_type: int
    silver: bool
    append_only: bool
    classification: str
    config_path: Path
    contract_path: Path
    contract: Contract
    raw: Mapping[str, Any] = field(repr=False, compare=False)

    @property
    def key(self) -> str:
        return f"{self.source}/{self.group}/{self.table}"


@dataclass(frozen=True)
class GroupConfig:
    source: str
    group: str
    description: str
    domain: str
    landing_folder: str
    path: Path
    tables: tuple[TableConfig, ...]

    @property
    def key(self) -> str:
        return f"{self.source}/{self.group}"


@dataclass(frozen=True)
class SourceConfig:
    source: str
    type: str
    description: str
    settings: Mapping[str, Any]
    defaults: Mapping[str, Any]
    path: Path
    groups: tuple[GroupConfig, ...]


@dataclass(frozen=True)
class CadenceProfile:
    name: str
    mode: str  # continuous | file_arrival | periodic | cron
    settings: Mapping[str, Any]

    @property
    def is_triggered(self) -> bool:
        return self.mode != "continuous"

    @property
    def ct_lookback_days(self) -> int | None:
        value = self.settings.get("ct_lookback_days")
        return int(value) if value is not None else None

    @property
    def max_gap_minutes(self) -> int:
        return int(self.settings.get("max_gap_minutes", 60))

    def max_gap_days(self) -> float | None:
        """Longest possible idle gap between runs, used for the file-events 7-day rule."""
        if self.mode == "continuous":
            return 0.0
        if self.mode == "file_arrival":
            return None  # driven by arrivals; idle source folders are covered by alerts
        if self.mode == "periodic":
            unit_days = {"HOURS": 1 / 24, "DAYS": 1.0, "WEEKS": 7.0, "MINUTES": 1 / 1440}
            return float(self.settings["interval"]) * unit_days[str(self.settings["unit"]).upper()]
        if self.mode == "cron":
            declared = self.settings.get("max_gap_days")
            return float(declared) if declared is not None else None
        raise ConfigError(f"cadence profile '{self.name}': unknown mode '{self.mode}'")


@dataclass(frozen=True)
class EnvConfig:
    name: str
    short: str
    raw: Mapping[str, Any] = field(repr=False)
    path: Path = field(default=Path())

    def _section(self, key: str) -> Mapping[str, Any]:
        value = self.raw.get(key) or {}
        if not isinstance(value, Mapping):
            raise ConfigError(f"{self.path}: '{key}' must be a mapping")
        return value

    @property
    def workspace_host(self) -> str:
        return str(self.raw.get("workspace_host") or "")

    @property
    def deployer_service_principal(self) -> str:
        return str(self._section("deployer").get("service_principal") or "")

    @property
    def catalogs(self) -> Mapping[str, str]:
        return self._section("catalogs")

    @property
    def landing(self) -> Mapping[str, str]:
        return self._section("landing")

    @property
    def cadence(self) -> Mapping[str, Any]:
        return self._section("cadence")

    @property
    def scope(self) -> Mapping[str, Any]:
        return self._section("scope")

    @property
    def governance(self) -> Mapping[str, Any]:
        return self._section("governance")

    @property
    def dqm(self) -> Mapping[str, Any]:
        return self._section("dqm")

    @property
    def alerts(self) -> Mapping[str, Any]:
        return self._section("alerts")

    @property
    def pipelines(self) -> Mapping[str, Any]:
        return self._section("pipelines")

    @property
    def seed_enabled(self) -> bool:
        return bool(self._section("seed").get("enabled", False))

    @property
    def storage(self) -> Mapping[str, Any]:
        return self._section("storage")

    @property
    def alert_emails(self) -> tuple[str, ...]:
        return tuple((self.alerts.get("notify") or {}).get("emails") or ())

    @property
    def alert_destination_ids(self) -> tuple[str, ...]:
        return tuple((self.alerts.get("notify") or {}).get("destination_ids") or ())

    @property
    def dqm_emails(self) -> tuple[str, ...]:
        return tuple((self.dqm.get("notify") or {}).get("emails") or ())

    @property
    def dqm_destination_ids(self) -> tuple[str, ...]:
        return tuple((self.dqm.get("notify") or {}).get("destination_ids") or ())


@dataclass(frozen=True)
class FrameworkConfig:
    raw: Mapping[str, Any] = field(repr=False)

    def section(self, key: str) -> Mapping[str, Any]:
        value = self.raw.get(key) or {}
        if not isinstance(value, Mapping):
            raise ConfigError(f"conf/framework.yml: '{key}' must be a mapping")
        return value

    @property
    def naming(self) -> Mapping[str, Any]:
        return self.section("naming")

    @property
    def limits(self) -> Mapping[str, Any]:
        return self.section("limits")

    @property
    def expectations(self) -> Mapping[str, Any]:
        return self.section("expectations")

    @property
    def tags(self) -> Mapping[str, Any]:
        return self.section("tags")

    def layer(self, layer: str) -> Mapping[str, Any]:
        return self.section("layers").get(layer) or {}

    def cadence_profile(self, name: str) -> CadenceProfile:
        profiles = self.section("cadence_profiles")
        if name not in profiles:
            raise ConfigError(f"unknown cadence profile '{name}' (known: {', '.join(sorted(profiles))})")
        settings = profiles[name]
        return CadenceProfile(name=name, mode=str(settings["mode"]), settings=settings)

    @property
    def cadence_profiles(self) -> tuple[CadenceProfile, ...]:
        return tuple(self.cadence_profile(n) for n in self.section("cadence_profiles"))
