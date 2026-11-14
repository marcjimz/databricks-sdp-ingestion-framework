# SDP Ingestion Framework — one-command, service-principal deploys of a config-driven bundle.
#
#   make deploy <env> PROFILE=<deployer CLI profile>
#
# <env> is a positional word: one of the files in conf/environments/ (sandbox dev tst prod).
# PROFILE is REQUIRED for anything that talks to a workspace, so every command states the
# identity it runs as; PROFILE=env uses your ambient login (e.g. a Databricks web terminal).
#
# Prerequisites are checked on EVERY workspace command (conf/prerequisites.yml):
#   deploy            strict  - stops while any required prerequisite is missing
#   validate / start  warn    - prints the same report and continues
#
# `make deploy <env>` = tools check -> preflight (strict) -> bundle validate -> bundle deploy
#                       -> governance job (registry, tags, audit) -> start pipelines

SHELL := /bin/bash
.DEFAULT_GOAL := help

ENVS   := $(sort $(basename $(filter-out %.local.yml,$(notdir $(wildcard conf/environments/*.yml)))))
TARGET := $(filter $(ENVS),$(MAKECMDGOALS))
PY     := uv run --quiet python

# PROFILE=<name>      a ~/.databrickscfg profile
# PROFILE=env         ambient DATABRICKS_* variables (e.g. CI, a Databricks web terminal)
# PROFILE=azure-cli   your `az login`, against the environment's workspace_host
ifdef PROFILE
ifeq ($(PROFILE),env)
unexport DATABRICKS_CONFIG_PROFILE
else ifeq ($(PROFILE),azure-cli)
unexport DATABRICKS_CONFIG_PROFILE
export DATABRICKS_AUTH_TYPE := azure-cli
export DATABRICKS_HOST := $(or $(DATABRICKS_HOST),$(shell $(PY) -m ingestion_framework.prereqs host --env $(TARGET) 2>/dev/null))
else
export DATABRICKS_CONFIG_PROFILE := $(PROFILE)
endif
endif
MIN_CLI := 1.24.0
comma := ,

# Bundle variables resolved from the environment config + workspace (warehouse id, deployer SP).
BVARS = $$($(PY) -m ingestion_framework.prereqs bundle-vars --env $(TARGET))

.PHONY: help install lint test typecheck fmt check tools guard-target guard-profile guard-infra infra-plan \
        infra-sandbox infra-destroy validate preflight \
        prereqs deploy governance start stop trace reload seed verify scaffold summary deck destroy $(ENVS)

help:
	@echo "SDP Ingestion Framework"
	@echo ""
	@echo "  Local (no workspace):"
	@echo "    make install                 create .venv with uv (dev dependencies)"
	@echo "    make check                   lint + typecheck + tests"
	@echo "    make lint                    ruff + framework lint (every table has a valid data contract)"
	@echo "    make test                    unit + enforcement tests"
	@echo "    make scaffold SOURCE=facets GROUP=claims FROM=csv|dfm INPUT=<file|dir> [OWNER=] [STATUS=]"
	@echo "    make trace <env> TABLE=<name|fqn> | PIPELINE=<name> [OFFLINE=1]"
	@echo "    make deck                    rebuild docs/deck/ingestion-framework-overview.pptx"
	@echo ""
	@echo "  Workspace (PROFILE=<cli profile> required):"
	@echo "    make prereqs   <env> PROFILE=<admin> [DRY_RUN=1]   apply automatable prerequisites"
	@echo "    make preflight <env> PROFILE=<deployer>            verify every prerequisite"
	@echo "    make deploy    <env> PROFILE=<deployer>            preflight -> validate -> deploy -> governance -> start"
	@echo "    make validate  <env> PROFILE=<deployer>"
	@echo "    make start|stop <env> PROFILE=<deployer>"
	@echo "    make governance <env> PROFILE=<deployer>           registry + tags + undeclared audit now"
	@echo "    make reload    <env> TABLE=<table> SINCE=<yyyyMMddHHmmss> PROFILE=<deployer>"
	@echo "    make seed      sandbox BATCH=full_load|changes|reload|reset [TABLE=<t>] PROFILE=<deployer>"
	@echo "    make verify    sandbox PROFILE=<deployer>          silver == expected synthetic state"
	@echo "    make summary   <env> PROFILE=<deployer>"
	@echo "    make destroy   <env> PROFILE=<deployer> CONFIRM=<env>   (refused for prod)"
	@echo ""
	@echo "  Sandbox infrastructure (Terraform, infra/azure-sandbox/terraform.tfvars):"
	@echo "    make infra-plan                 what would be created"
	@echo "    make infra-sandbox [AUTO_APPROVE=1]   workspace + storage + identity, writes sandbox.local.yml"
	@echo "    make infra-destroy CONFIRM=infra"
	@echo ""
	@echo "  PROFILE=azure-cli authenticates with your az login against the env's workspace_host."
	@echo ""
	@echo "  <env>: $(ENVS)"

# ---- local ------------------------------------------------------------------------------
install:
	uv sync

lint:
	uv run --quiet ruff check src tests resources
	uv run --quiet ruff format --check src tests resources
	$(PY) -m ingestion_framework.lint

typecheck:
	uv run --quiet mypy

test:
	uv run --quiet pytest

fmt:
	uv run --quiet ruff format src tests resources
	uv run --quiet ruff check --fix src tests resources

check: lint typecheck test

scaffold:
	@if [ -z "$(SOURCE)" ] || [ -z "$(GROUP)" ] || [ -z "$(FROM)" ] || [ -z "$(INPUT)" ]; then \
	  echo "usage: make scaffold SOURCE=facets GROUP=claims FROM=csv|dfm INPUT=<file|dir> [OWNER=team] [STATUS=draft]"; exit 2; fi
	$(PY) -m ingestion_framework.scaffold --source $(SOURCE) --group $(GROUP) --from $(FROM) --path $(INPUT) \
	  $(if $(OWNER),--owner $(OWNER)) $(if $(STATUS),--status $(STATUS))
	$(PY) -m ingestion_framework.lint

deck:
	uv run --quiet --with python-pptx python docs/deck/build_deck.py

# ---- guards -----------------------------------------------------------------------------
# Local tool prerequisites, checked before anything touches a workspace.
tools:
	@command -v uv >/dev/null || { echo "error: uv is required (https://docs.astral.sh/uv/)"; exit 2; }
	@command -v databricks >/dev/null || { echo "error: Databricks CLI >= $(MIN_CLI) is required"; exit 2; }
	@v=$$(databricks version | sed -E 's/.*v([0-9.]+).*/\1/'); \
	 if [ "$$(printf '%s\n%s\n' "$(MIN_CLI)" "$$v" | sort -V | head -1)" != "$(MIN_CLI)" ]; then \
	   echo "error: Databricks CLI $$v < $(MIN_CLI) (needed for data_quality_monitors + direct engine)"; \
	   echo "       brew upgrade databricks   (or: https://docs.databricks.com/dev-tools/cli/install)"; exit 2; fi

guard-target:
	@if [ -z "$(TARGET)" ]; then echo "error: name an environment, e.g. 'make $(firstword $(MAKECMDGOALS)) sandbox'"; \
	  echo "       environments: $(ENVS)"; exit 2; fi
	@if [ $(words $(TARGET)) -gt 1 ]; then echo "error: pick ONE environment, got: $(TARGET)"; exit 2; fi

guard-profile: guard-target
	@if [ -z "$(PROFILE)" ]; then \
	  echo "error: PROFILE=<cli profile> is required, e.g. 'make $(firstword $(MAKECMDGOALS)) $(TARGET) PROFILE=<profile>'"; \
	  echo "       PROFILE=env uses your ambient login. Your profiles:"; \
	  databricks auth profiles 2>/dev/null | awk 'NR>1{print "         " $$1}'; exit 2; fi
	@echo "[make] env=$(TARGET) profile=$(PROFILE)"

# ---- prerequisites ----------------------------------------------------------------------
preflight: tools guard-profile
	$(PY) -m ingestion_framework.prereqs preflight --env $(TARGET) --strict

prereqs: tools guard-profile
	$(PY) -m ingestion_framework.prereqs apply --env $(TARGET) $(if $(DRY_RUN),--dry-run)
	@echo ""; echo "Now verify as the deployer: make preflight $(TARGET) PROFILE=<deployer profile>"

# Warn-mode report used by non-deploy targets (never blocks).
define warn_prereqs
	@$(PY) -m ingestion_framework.prereqs preflight --env $(TARGET) || true
endef

# ---- bundle lifecycle -------------------------------------------------------------------
validate: tools guard-profile
	$(warn_prereqs)
	databricks bundle validate -t $(TARGET) $(BVARS)

deploy: tools guard-profile
	$(PY) -m ingestion_framework.lint
	$(PY) -m ingestion_framework.prereqs preflight --env $(TARGET) --strict
	databricks bundle validate -t $(TARGET) $(BVARS)
	databricks bundle deploy   -t $(TARGET) $(BVARS)
	databricks bundle run      -t $(TARGET) $(BVARS) job_governance
	$(PY) -m ingestion_framework.ops start --env $(TARGET) $(patsubst --var=%,--var %,$(BVARS))
	@echo ""; echo "deployed $(TARGET). Trace any table: make trace $(TARGET) TABLE=<name> PROFILE=$(PROFILE)"

governance: tools guard-profile
	$(warn_prereqs)
	databricks bundle run -t $(TARGET) $(BVARS) job_governance

start: tools guard-profile
	$(warn_prereqs)
	$(PY) -m ingestion_framework.ops start --env $(TARGET) $(patsubst --var=%,--var %,$(BVARS))

stop: tools guard-profile
	$(PY) -m ingestion_framework.ops stop --env $(TARGET) $(patsubst --var=%,--var %,$(BVARS))

reload: tools guard-profile
	@if [ -z "$(TABLE)" ] || [ -z "$(SINCE)" ]; then \
	  echo "usage: make reload <env> TABLE=<table> SINCE=<qlik reload start, yyyyMMddHHmmss UTC> PROFILE=<p>"; exit 2; fi
	$(PY) -m ingestion_framework.ops reload --env $(TARGET) --table $(TABLE) --since $(SINCE) \
	  $(patsubst --var=%,--var %,$(BVARS))

summary: tools guard-profile
	databricks bundle summary -t $(TARGET) $(BVARS)

trace: guard-target
	$(PY) -m ingestion_framework.trace --env $(TARGET) $(if $(TABLE),--table $(TABLE)) \
	  $(if $(PIPELINE),--pipeline $(PIPELINE)) $(if $(or $(OFFLINE),$(if $(PROFILE),,1)),--offline)

# ---- sandbox infrastructure (infra/) --------------------------------------------------------
# A dedicated sandbox, as Terraform: resource group, Premium workspace (assigned to the regional
# metastore), access connector, ADLS landing account + file-events roles. Values: infra/azure-sandbox/
# terraform.tfvars (gitignored). Outputs are written into conf/environments/sandbox.local.yml.
INFRA_DIR ?= infra/azure-sandbox
TF        := terraform -chdir=$(INFRA_DIR)

guard-infra:
	@command -v terraform >/dev/null || { echo "error: terraform >= 1.5 is required for infra/"; exit 2; }
	@command -v az >/dev/null || { echo "error: the Azure CLI is required (az login to the sandbox tenant)"; exit 2; }
	@if [ ! -f $(INFRA_DIR)/terraform.tfvars ]; then \
	  echo "error: cp $(INFRA_DIR)/terraform.tfvars.example $(INFRA_DIR)/terraform.tfvars and fill it in"; exit 2; fi

infra-plan: guard-infra
	$(TF) init -input=false -upgrade
	$(TF) plan -input=false

infra-sandbox: guard-infra
	$(TF) init -input=false -upgrade
	$(TF) apply -input=false $(if $(AUTO_APPROVE),-auto-approve)
	$(PY) -m ingestion_framework.prereqs.local_env --env sandbox --terraform $(INFRA_DIR)
	@echo ""; echo "next: make prereqs sandbox PROFILE=azure-cli   then   make preflight sandbox PROFILE=<deployer>"

infra-destroy: guard-infra
	@if [ "$(CONFIRM)" != "infra" ]; then \
	  echo "error: add CONFIRM=infra to delete the sandbox workspace, storage account and everything in them"; exit 2; fi
	$(TF) destroy -input=false $(if $(AUTO_APPROVE),-auto-approve)

# ---- sandbox only -----------------------------------------------------------------------
BATCH ?= full_load
seed: tools guard-profile
	@if [ "$(TARGET)" != "sandbox" ]; then echo "error: seed is sandbox-only"; exit 2; fi
	databricks bundle run -t $(TARGET) $(BVARS) job_seed --params batch=$(BATCH)$(if $(TABLE),$(comma)table=$(TABLE))

verify: tools guard-profile
	@if [ "$(TARGET)" != "sandbox" ]; then echo "error: verify is sandbox-only"; exit 2; fi
	databricks bundle run -t $(TARGET) $(BVARS) job_seed --params batch=verify

destroy: tools guard-profile
	@if [ "$(TARGET)" = "prod" ]; then echo "error: destroy is refused for prod"; exit 2; fi
	@if [ "$(CONFIRM)" != "$(TARGET)" ]; then echo "error: add CONFIRM=$(TARGET) to destroy every $(TARGET) resource"; exit 2; fi
	databricks bundle destroy -t $(TARGET) $(BVARS) --auto-approve

# Environment words are inert goals consumed by TARGET above.
$(ENVS):
	@:
