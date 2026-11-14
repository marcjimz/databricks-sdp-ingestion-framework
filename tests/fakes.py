"""Recording fakes for the SDP Python API (`pyspark.pipelines`) and the Spark reader.

They capture exactly what a pipeline would register so tests can assert on the graph without
a Databricks runtime (create_auto_cdc_flow is Databricks-only).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class FakeFrame:
    source: str
    kind: str  # "files" | "table"
    format: str | None = None
    options: dict[str, str] = field(default_factory=dict)
    schema: str | None = None
    ops: list[tuple[str, Any]] = field(default_factory=list)

    def selectExpr(self, *exprs: str) -> FakeFrame:
        return FakeFrame(self.source, self.kind, self.format, self.options, self.schema, [*self.ops, ("select", exprs)])

    def where(self, condition: str) -> FakeFrame:
        return FakeFrame(
            self.source, self.kind, self.format, self.options, self.schema, [*self.ops, ("where", condition)]
        )


class _Reader:
    def __init__(self) -> None:
        self._format: str | None = None
        self._options: dict[str, str] = {}
        self._schema: str | None = None

    def format(self, fmt: str) -> _Reader:
        self._format = fmt
        return self

    def options(self, **opts: str) -> _Reader:
        self._options.update(opts)
        return self

    def schema(self, ddl: str) -> _Reader:
        self._schema = ddl
        return self

    def load(self, path: str) -> FakeFrame:
        return FakeFrame(path, "files", self._format, dict(self._options), self._schema)

    def table(self, name: str) -> FakeFrame:
        return FakeFrame(name, "table")


class FakeSpark:
    @property
    def readStream(self) -> _Reader:
        return _Reader()


@dataclass
class FakeDP:
    streaming_tables: dict[str, dict[str, Any]] = field(default_factory=dict)
    flows: list[dict[str, Any]] = field(default_factory=list)
    views: dict[str, dict[str, Any]] = field(default_factory=dict)
    cdc_flows: list[dict[str, Any]] = field(default_factory=list)

    def create_streaming_table(self, name: str, **kwargs: Any) -> None:
        assert name not in self.streaming_tables, f"duplicate streaming table {name}"
        self.streaming_tables[name] = kwargs

    def append_flow(self, *, target: str, name: str) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
        def register(fn: Callable[[], Any]) -> Callable[[], Any]:
            self.flows.append({"target": target, "name": name, "fn": fn})
            return fn

        return register

    def temporary_view(self, *, name: str, comment: str | None = None) -> Callable[[Callable[[], Any]], Any]:
        def register(fn: Callable[[], Any]) -> Callable[[], Any]:
            self.views[name] = {
                "fn": fn,
                "warn": getattr(fn, "_expect_all", {}),
                "fail": getattr(fn, "_expect_all_or_fail", {}),
            }
            return fn

        return register

    @staticmethod
    def expect_all(expectations: dict[str, str]) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
        def deco(fn: Callable[[], Any]) -> Callable[[], Any]:
            fn._expect_all = dict(expectations)  # type: ignore[attr-defined]
            return fn

        return deco

    @staticmethod
    def expect_all_or_fail(expectations: dict[str, str]) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
        def deco(fn: Callable[[], Any]) -> Callable[[], Any]:
            fn._expect_all_or_fail = dict(expectations)  # type: ignore[attr-defined]
            return fn

        return deco

    def create_auto_cdc_flow(self, **kwargs: Any) -> None:
        self.cdc_flows.append(kwargs)
