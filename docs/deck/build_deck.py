"""Rebuild the overview deck from docs/deck/slides.json (`make deck`).

slides.json is the source of truth; the .pptx next to it is committed so anyone can open it.
Rendering uses the Databricks-branded generator from the `databricks-slides` plugin; point
SLIDES_GENERATOR at its generate-pptx.py if it is not in the default location.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT = (
    Path.home() / ".claude/plugins/marketplaces/plugin-marketplace/experimental/general/databricks-slides"
    / "skills/slide-deck/scripts/generate-pptx.py"
)


def main() -> int:
    generator = Path(os.environ.get("SLIDES_GENERATOR") or DEFAULT)
    if not generator.exists():
        print(f"deck: generator not found at {generator}; set SLIDES_GENERATOR=<path to generate-pptx.py>")
        print(f"deck: the committed deck is still at {HERE / 'ingestion-framework-overview.pptx'}")
        return 1
    out = HERE / "ingestion-framework-overview.pptx"
    return subprocess.call(
        [sys.executable, str(generator), "--input", str(HERE / "slides.json"), "--output", str(out)]
    )


if __name__ == "__main__":
    raise SystemExit(main())
