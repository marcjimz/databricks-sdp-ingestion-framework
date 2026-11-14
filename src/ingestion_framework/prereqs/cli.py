"""Thin wrapper around the Databricks CLI, so preflight uses exactly the PROFILE the Makefile was given."""

from __future__ import annotations

import json
import shutil
import subprocess
from typing import Any, Protocol


class CliError(RuntimeError):
    pass


class Cli(Protocol):
    def json(self, *args: str) -> Any: ...
    def text(self, *args: str) -> str: ...
    def run(self, *args: str) -> None: ...


class DatabricksCli:
    """Calls ``databricks`` with ``-o json``; auth comes from DATABRICKS_CONFIG_PROFILE (set by make)."""

    def __init__(self, binary: str = "databricks") -> None:
        found = shutil.which(binary)
        if not found:
            raise CliError("databricks CLI not found on PATH")
        self.binary = found

    def _exec(self, args: list[str]) -> str:
        proc = subprocess.run([self.binary, *args], capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            lines = [ln.strip() for ln in (proc.stderr or proc.stdout).strip().splitlines() if ln.strip()]
            errors = [ln for ln in lines if ln.startswith("Error")]
            detail = errors[0] if errors else " | ".join(lines[:3])
            raise CliError(f"`databricks {' '.join(args[:3])}`: {detail or f'exit {proc.returncode}'}")
        return proc.stdout

    def json(self, *args: str) -> Any:
        out = self._exec([*args, "-o", "json"])
        return json.loads(out) if out.strip() else {}

    def text(self, *args: str) -> str:
        return self._exec(list(args)).strip()

    def run(self, *args: str) -> None:
        self._exec(list(args))
