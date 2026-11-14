"""The resolved configuration for one environment: the single object everything else reads."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ingestion_framework.config.models import (
    CadenceProfile,
    ConfigError,
    EnvConfig,
    FrameworkConfig,
    GroupConfig,
    SourceConfig,
    TableConfig,
)

LAYERS = ("bronze", "silver")


@dataclass(frozen=True)
class ResolvedConfig:
    root: Path
    framework: FrameworkConfig
    env: EnvConfig
    sources: tuple[SourceConfig, ...]

    # ---- scope -------------------------------------------------------------------
    def _in_scope(self, kind: str, key: str) -> bool:
        allowed = self.env.scope.get(kind) or []
        return not allowed or key in allowed

    def sources_in_scope(self) -> list[SourceConfig]:
        return [s for s in self.sources if self._in_scope("sources", s.source)]

    def source(self, name: str) -> SourceConfig:
        for s in self.sources:
            if s.source == name:
                return s
        raise ConfigError(f"unknown source '{name}'")

    def tables_in_scope(self, group: GroupConfig) -> tuple[TableConfig, ...]:
        return tuple(t for t in group.tables if t.enabled and self._in_scope("tables", t.key))

    def groups_in_scope(self) -> list[GroupConfig]:
        """Groups in scope with their tables filtered to enabled + in-scope tables."""
        result: list[GroupConfig] = []
        for src in self.sources_in_scope():
            for g in src.groups:
                if not self._in_scope("groups", g.key):
                    continue
                tables = self.tables_in_scope(g)
                if tables:
                    result.append(
                        GroupConfig(
                            source=g.source,
                            group=g.group,
                            description=g.description,
                            domain=g.domain,
                            landing_folder=g.landing_folder,
                            path=g.path,
                            tables=tables,
                        )
                    )
        return result

    def group(self, source: str, group: str) -> GroupConfig:
        for g in self.groups_in_scope():
            if g.source == source and g.group == group:
                return g
        raise ConfigError(f"group '{source}/{group}' is not declared or not in scope for env '{self.env.name}'")

    # ---- naming ------------------------------------------------------------------
    def _fmt(self, key: str, **kwargs: str) -> str:
        return str(self.framework.naming[key]).format(env=self.env.name, **kwargs)

    def catalog(self, layer: str) -> str:
        if layer not in LAYERS:
            raise ConfigError(f"unknown layer '{layer}'")
        name = self.env.catalogs.get(layer)
        if not name:
            raise ConfigError(f"{self.env.path}: catalogs.{layer} is required")
        return str(name)

    def schema(self, source: str) -> str:
        return self._fmt("schema", source=source)

    @property
    def ops_schema(self) -> str:
        return str(self.framework.naming["ops_schema"])

    @property
    def ops_schema_fqn(self) -> str:
        return f"{self.catalog('bronze')}.{self.ops_schema}"

    def table_fqn(self, layer: str, source: str, name: str) -> str:
        return f"{self.catalog(layer)}.{self.schema(source)}.{name}"

    def pipeline_name(self, source: str, group: str) -> str:
        return self._fmt("pipeline", source=source, group=group)

    def job_name(self, source: str, group: str) -> str:
        return self._fmt("job", source=source, group=group)

    def event_log_fqn(self, source: str, group: str) -> str:
        return f"{self.ops_schema_fqn}.{self._fmt('event_log', source=source, group=group)}"

    def landing_root(self, source: str) -> str:
        root = self.env.landing.get(source)
        if not root:
            raise ConfigError(f"{self.env.path}: landing.{source} is required")
        return str(root).rstrip("/")

    def group_landing(self, group: GroupConfig) -> str:
        return f"{self.landing_root(group.source)}/{group.landing_folder}"

    # ---- cadence -----------------------------------------------------------------
    def cadence_for(self, source: str, group: str) -> CadenceProfile:
        overrides = self.env.cadence.get("overrides") or {}
        name = overrides.get(f"{source}/{group}") or self.env.cadence.get("default")
        if not name:
            raise ConfigError(f"{self.env.path}: cadence.default is required")
        return self.framework.cadence_profile(str(name))

    # ---- tags --------------------------------------------------------------------
    @property
    def tag_prefix(self) -> str:
        return str(self.env.governance.get("tag_prefix") or "")

    def tag_key(self, logical_key: str) -> str:
        return f"{self.tag_prefix}{logical_key}"

    @property
    def governed_tags_enabled(self) -> bool:
        return bool(self.env.governance.get("governed_tags", True))

    def rel(self, path: Path) -> str:
        """Repo-relative POSIX path, used in tags and the registry."""
        return path.resolve().relative_to(self.root.resolve()).as_posix()
