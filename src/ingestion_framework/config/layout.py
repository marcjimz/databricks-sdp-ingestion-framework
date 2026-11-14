"""Where declarations live on disk. The only module that knows the folder layout.

    sources/<source>/
      source.yml                          the source: adapter type, conventions, defaults
      groups/<group>/
        group.yml                         one group = one pipeline
        tables/<table>/
          table.yml                       ops config (inherits the source defaults)
          contract.odcs.yaml              ODCS data contract

One folder per table keeps every folder small and makes a path read source -> group -> table.
"""

from __future__ import annotations

from pathlib import Path

SOURCE_FILE = "source.yml"
GROUPS_DIR = "groups"
GROUP_FILE = "group.yml"
TABLES_DIR = "tables"
TABLE_FILE = "table.yml"
CONTRACT_FILE = "contract.odcs.yaml"

# For globbing from the repo root.
GROUP_GLOB = f"sources/*/{GROUPS_DIR}/*"
CONTRACT_GLOB = f"sources/*/{GROUPS_DIR}/*/{TABLES_DIR}/*/{CONTRACT_FILE}"


def group_dir(root: Path, source: str, group: str) -> Path:
    return root / "sources" / source / GROUPS_DIR / group


def table_dir(gdir: Path, name: str) -> Path:
    return gdir / TABLES_DIR / name


def subdirs(path: Path) -> list[Path]:
    """Sorted child folders. Only hidden (dot) folders are skipped: anything else is a declaration and
    must be complete, or the loader fails. A silently skipped group would delete its pipeline on deploy."""
    if not path.is_dir():
        return []
    return sorted(p for p in path.iterdir() if p.is_dir() and not p.name.startswith("."))
