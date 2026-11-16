# Thin wrappers around the Databricks CLI and Terraform.
#   TARGET   bundle target: sandbox (default) | dev | prod
#   PROFILE  Databricks CLI profile for dev/prod. The sandbox needs none: its workspace URL comes from
#            Terraform and auth from your `az login`, so nothing is written to ~/.databrickscfg.
TARGET ?= sandbox
BATCH ?= full
PREFIX ?= lffsbx
INFRA := infra/azure-sandbox

ifeq ($(TARGET)$(PROFILE),sandbox)
export DATABRICKS_HOST := $(shell terraform -chdir=$(INFRA) output -raw workspace_url 2>/dev/null)
export DATABRICKS_AUTH_TYPE := azure-cli
endif
CLI := databricks $(if $(PROFILE),--profile $(PROFILE))

# The framework version is the pinned submodule's; both bundles use it, so they cannot drift apart.
FRAMEWORK_VERSION := $(shell cat lakeflow_framework/VERSION 2>/dev/null)
# Exported outside this repo: its .gitignore (.databricks/) would hide the files from the framework deploy.
FRAMEWORK_DIR := $(or $(TMPDIR),/tmp)/lakeflow_framework/$(FRAMEWORK_VERSION)
export BUNDLE_VAR_framework_version := $(FRAMEWORK_VERSION)

.PHONY: check validate infra framework deploy seed run destroy

check: ## offline: specs against the framework's JSON schemas, and the customer-name guard
	uv run --quiet --no-project --with jsonschema==4.26.0 python lakeflow_framework/scripts/validate_dataflows.py src/dataflows
	python3 scripts/check_repo.py

validate: check ## + bundle validate against the target workspace
	$(CLI) bundle validate --target $(TARGET)

infra: ## sandbox only: Azure workspace + ADLS (Terraform), then the Unity Catalog objects
	terraform -chdir=$(INFRA) init -input=false
	terraform -chdir=$(INFRA) apply -input=false -var prefix=$(PREFIX)
	DATABRICKS_HOST=$$(terraform -chdir=$(INFRA) output -raw workspace_url) DATABRICKS_AUTH_TYPE=azure-cli \
	  $(INFRA)/unity_catalog.sh

framework: ## deploy the pinned Lakeflow Framework to the workspace (once per workspace and version)
	@# Deployed from an export of the pinned commit: the Databricks CLI cannot deploy from inside a git submodule.
	mkdir -p $(FRAMEWORK_DIR)
	git -C lakeflow_framework archive HEAD | tar -x -C $(FRAMEWORK_DIR)
	cd $(FRAMEWORK_DIR) && $(CLI) bundle deploy --target dev --var version=$(FRAMEWORK_VERSION)

deploy: ## deploy this bundle: two pipelines and the job (the sandbox also gets the synthetic file job)
	$(CLI) bundle deploy --target $(TARGET)

seed: ## sandbox only: write Qlik-shaped files, BATCH=full | changes
	@test "$(TARGET)" = sandbox || { echo "seed is sandbox only"; exit 2; }
	$(CLI) bundle run --target $(TARGET) synthetic_qlik_files --params batch=$(BATCH)

run: ## run the job: bronze pipeline, then silver
	$(CLI) bundle run --target $(TARGET) facets_ingestion

destroy: ## remove this bundle's pipelines and jobs; deleting a pipeline also drops its tables (landing files stay)
	$(CLI) bundle destroy --target $(TARGET)
