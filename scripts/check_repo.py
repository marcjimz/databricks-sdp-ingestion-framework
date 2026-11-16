"""Fail if any committed or new file names the customer this repo was built for.

The names are stored only as SHA-256 hashes, so this check never names them itself. Words are matched
lower-cased, alone and joined with their neighbour ("foo bar" -> "foobar", "foo-bar").
The framework submodule (lakeflow_framework/) is upstream code and is not scanned.
"""

from __future__ import annotations

import hashlib
import itertools
import re
import subprocess
import sys
from pathlib import Path

FORBIDDEN_SHA256 = {
    "0914df4ef3a50c809086b6bef2af0614a258abddd44470183e4d7f1cfd650c5e",
    "97d6944fd76d4bdf7b2bb03b2362fc08e4b880557b4d49135486ea43c4c37bf6",
    "043ccbf57df48e37e363ad07d0c1bd9e5a62422a0b9667eab02374bc749261dd",
}
TEXT = {".py", ".yml", ".yaml", ".json", ".md", ".sh", ".tf", ".example", ".txt", ".sql", ""}


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    files = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard"],
                           cwd=root, capture_output=True, text=True, check=True).stdout.split()
    hits = []
    for rel in files:
        path = root / rel
        if not path.is_file() or path.suffix.lower() not in TEXT:
            continue
        words = re.findall(r"[a-z0-9]+", path.read_text(errors="ignore").lower())
        pairs = list(itertools.pairwise(words))
        candidates = set(words) | {a + b for a, b in pairs} | {f"{a}-{b}" for a, b in pairs}
        if any(hashlib.sha256(w.encode()).hexdigest() in FORBIDDEN_SHA256 for w in candidates):
            hits.append(rel)
    if hits:
        print(f"customer identifier found in: {', '.join(hits)}")
        return 1
    print(f"name check: {len(files)} files clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
