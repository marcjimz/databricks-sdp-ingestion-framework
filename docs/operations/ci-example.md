# CI example (for your platform team to adopt)

This repo deliberately ships **no CI/CD configuration**; those pipelines belong to the platform team. Every gate
below is a Makefile target, so any CI system can run it. Here is a reference GitHub Actions job to copy:

```yaml
# .github/workflows/ingestion-framework.yml  (example — not included in this repo)
name: ingestion-framework
on: [pull_request]
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
      - run: make install
      - run: make check            # ruff, framework lint (contracts), mypy, unit + enforcement tests
  validate:
    needs: check
    runs-on: ubuntu-latest
    environment: dev
    env:
      DATABRICKS_HOST: ${{ vars.DEV_HOST }}
      DATABRICKS_CLIENT_ID: ${{ secrets.DEV_DEPLOYER_CLIENT_ID }}
      DATABRICKS_CLIENT_SECRET: ${{ secrets.DEV_DEPLOYER_CLIENT_SECRET }}
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
      - uses: databricks/setup-cli@main
      - run: make install
      - run: make preflight dev PROFILE=env      # PROFILE=env = use the DATABRICKS_* variables above
      - run: make validate  dev PROFILE=env
```

Recommended: deploys to `tst`/`prod` run from a protected branch with `make deploy <env> PROFILE=env`, using that
environment's deployer service principal.

The enforcement tests are the contract-level gate:
- every table has a valid ODCS contract;
- ingestion is event-driven;
- DQM and alerts exist in every environment;
- prod is protected;
- no customer identifiers appear.
