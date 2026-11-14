"""Write infrastructure outputs into the gitignored conf/environments/<env>.local.yml.

`make infra-sandbox` provisions storage with Terraform (infra/<cloud>-sandbox) and then calls this,
so the sandbox environment points at exactly what was built, without committing personal values.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

from ingestion_framework.config import deep_merge


def local_env_from_output(output: dict[str, Any]) -> dict[str, Any]:
    """Map the Terraform `sandbox_env` output to environment overrides."""
    landing = {k.removeprefix("landing_"): v for k, v in output.items() if k.startswith("landing_")}
    doc: dict[str, Any] = {"landing": landing, "storage": {"create": True, **output.get("storage", {})}}
    if output.get("workspace_host"):
        doc["workspace_host"] = output["workspace_host"]
    if output.get("account_id"):  # lets `make prereqs` grant the deployer ASSIGN on governed tags
        doc["governance"] = {"account_id": output["account_id"]}
    return doc


def merge_local_env(path: Path, updates: dict[str, Any]) -> None:
    current = yaml.safe_load(path.read_text()) if path.exists() else {}
    current = current or {}
    current.pop("storage", None)  # replaced wholesale: never mix one cloud's keys with another's
    merged = deep_merge(current, updates)
    header = "# Personal / provisioned values for this environment (gitignored). Deep-merged over <env>.yml.\n"
    path.write_text(header + yaml.safe_dump(merged, sort_keys=False))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--env", required=True)
    ap.add_argument("--root", type=Path, default=Path.cwd())
    ap.add_argument("--terraform", type=Path, required=True, help="Terraform module dir with a sandbox_env output")
    args = ap.parse_args(argv)
    proc = subprocess.run(
        ["terraform", f"-chdir={args.terraform}", "output", "-json", "sandbox_env"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        print(f"local-env: terraform output failed: {proc.stderr.strip()}", file=sys.stderr)
        return 2
    path = args.root / "conf" / "environments" / f"{args.env}.local.yml"
    merge_local_env(path, local_env_from_output(json.loads(proc.stdout)))
    print(f"local-env: wrote workspace_host, landing and storage to {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
