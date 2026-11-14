"""Repo-wide guarantees: contracts everywhere, prerequisites implemented, customer anonymity."""

from __future__ import annotations

import hashlib
import itertools
import re
import subprocess

import pytest

from ingestion_framework.config import read_yaml
from ingestion_framework.lint import lint_repo
from ingestion_framework.prereqs import CHECKS
from tests.conftest import REPO_ROOT

pytestmark = pytest.mark.enforcement

# SHA-256 of identifiers that must never appear in this repo (the customer it was built for).
# Stored as hashes so this test itself never names them. Matched on lower-cased single words and
# on adjacent-word concatenations ("foo bar" -> "foobar").
FORBIDDEN_SHA256 = {
    "0914df4ef3a50c809086b6bef2af0614a258abddd44470183e4d7f1cfd650c5e",
    "97d6944fd76d4bdf7b2bb03b2362fc08e4b880557b4d49135486ea43c4c37bf6",
    "043ccbf57df48e37e363ad07d0c1bd9e5a62422a0b9667eab02374bc749261dd",
}
TEXT_SUFFIXES = {".py", ".yml", ".yaml", ".md", ".toml", ".json", ".csv", ".dfm", ".txt", ".cfg", ""}


def test_repo_lint_is_clean():
    errors = [f"{p.path}: {p.message}" for p in lint_repo(REPO_ROOT) if p.severity == "error"]
    assert errors == []


def test_every_declared_prerequisite_is_implemented_and_vice_versa():
    declared = set(read_yaml(REPO_ROOT / "conf" / "prerequisites.yml")["checks"])
    assert declared == set(CHECKS)


def test_every_declared_prerequisite_has_owner_fix_and_severity():
    for check_id, meta in read_yaml(REPO_ROOT / "conf" / "prerequisites.yml")["checks"].items():
        for key in ("title", "severity", "owner", "fix"):
            assert meta.get(key), (check_id, key)
        assert meta["severity"] in {"required", "recommended"}, check_id


def _tracked_and_new_files():
    out = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    return [REPO_ROOT / p for p in out]


def test_no_customer_identifiers_anywhere():
    hits = []
    for path in _tracked_and_new_files():
        if path.suffix.lower() not in TEXT_SUFFIXES or not path.is_file():
            continue
        words = re.findall(r"[a-z0-9]+", path.read_text(errors="ignore").lower())
        pairs = list(itertools.pairwise(words))
        candidates = set(words) | {a + b for a, b in pairs}
        candidates |= {f"{a}-{b}" for a, b in pairs}
        if any(hashlib.sha256(w.encode()).hexdigest() in FORBIDDEN_SHA256 for w in candidates):
            hits.append(str(path.relative_to(REPO_ROOT)))
    assert hits == [], f"customer identifier found in: {hits}"
