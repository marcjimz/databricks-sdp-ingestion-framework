# Runbook

All commands take a positional environment and `PROFILE=<deployer profile>`.

## Deploy / redeploy

```bash
make deploy <env> PROFILE=<p>
```

Runs, in order: tools check → **preflight (strict)** → lint → `bundle validate` → `bundle deploy` → governance
job → start.

Redeploys are safe. Pipeline keys are stable, and prod pipelines and schemas are `prevent_destroy`.

## Start / stop

```bash
make start <env>    # continuous pipelines: start; triggered groups: run their trigger job once
make stop  <env>    # stop all framework pipelines
```

File-arrival triggers fire only for files that arrive *after* the trigger exists. `start` drains anything that
was already there.

## Qlik reloaded a table

A Qlik reload rewrites the table's full load. Older change rows must stop applying, or deleted rows could
resurface.

```bash
make reload <env> TABLE=cmc_clcl_claim SINCE=20261113100000 PROFILE=<p>   # reload start, UTC
```

This does two things:

1. Records the cutoff in `ingest_ops.reload_cutoffs`.
2. Full-refreshes **only the silver table**. The silver view then keeps only the snapshot written at or after
   the cutoff plus changes committed at or after it.

Bronze keeps the complete history and is never reset, so this works in prod too.

Qlik may write the new snapshot under the same name as the old one (`LOAD00000001`). The full-load flow sets
`cloudFiles.allowOverwrites`, so a rewritten file is ingested again; its newer file time puts it after the cutoff.
Wait for the reload's own pipeline update to finish before running `make reload`, because a pipeline accepts one
update at a time.

## Changing Auto Loader options on a running stream

Some Auto Loader options change what the managed file-events cache sends a stream; `allowOverwrites` is one.
Changing such an option on a stream that already exists makes it fail with
`CF_MANAGED_FILE_EVENTS_INVALID_CONTINUATION_TOKEN` (`include_updates and include_deletes cannot change between
requests`). This was seen live when the full-load flows gained `allowOverwrites`. To recover:

- **sandbox / dev:** full-refresh the pipeline (`databricks pipelines start-update <id> --full-refresh`). Bronze is
  rebuilt from the landing files.
- **prod**, where bronze is the system of record: reset only that flow's checkpoint, then treat it as a reload,
  because the flow reads its folder again:
  `databricks pipelines start-update <id> --json '{"reset_checkpoint_selection": ["<catalog>.<schema>.<table>__full_load"]}'`.
  Not yet tested live.

Avoid the problem by setting reader options when a table is first onboarded.

## A source column was added

It appears in bronze `_rescued_data`. To promote it:

1. Re-scaffold or edit the contract, then bump `version`.
2. Run `make lint`.
3. Deploy.

## A pipeline failed

1. Open the pipeline UI from `make trace <env> TABLE=<t>`, or check the notification if the environment has
   recipients. The errors are also in the event log, `ingest_ops.event_log_<source>_<group>`.
2. Triggered groups: the cadence job retries a failed update. Cancel it with `databricks jobs cancel-all-runs
   --job-id <id>` before starting a manual fix, or the retry holds the pipeline (one update at a time).
3. A `fail` expectation stops an update by design. The event log names the expectation and the contract.
4. If the problem is source data, fix it upstream. If the contract is wrong, fix the contract and redeploy.

## An "undeclared objects" alert fired

Someone created a table in a framework-owned schema. `ingest_ops.undeclared_objects` lists it. Either declare it
(scaffold it), or drop it and review who holds CREATE TABLE on the schema. Only the deployer should.

## Tag drift alert fired

Run `make governance <env>`. If it recurs, someone is editing tags by hand. Governed tag policies plus ASSIGN
permissions prevent that.

## Removing a group (taking it out of `scope`)

Taking a group out of scope deletes its pipeline and cadence job on the next deploy, but **its tables are kept**.
This was verified live, and it is the safe default: no data is lost silently. They then show up as
**undeclared objects** in `ingest_ops.undeclared_objects` and in the undeclared-tables alert. Then either:

- put the group back in scope, and the pipeline resumes on the same tables; or
- drop the tables deliberately, as the deployer (`DROP TABLE <catalog>.<source>.<table>`). In prod, do this only
  after the agreed retention period.
