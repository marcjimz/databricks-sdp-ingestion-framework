# Silver migration and cutover

Target state: the **new bronze catalog** is fully represented by the framework, and **silver tables are declared
by pipelines**. They replace the legacy silver tables that are maintained by hand today.

> This round covers the approach. Parity tooling (an automated legacy-vs-new comparison job) is the next
> increment.

## Phases

| Phase | What happens | Exit criterion |
|---|---|---|
| **1. Land bronze** | Deploy groups to `bronze_<env>.facets`; Qlik keeps feeding the legacy path in parallel | bronze row counts track the source; DQM freshness healthy |
| **2. Build silver alongside** | Framework silver in `silver_<env>.facets`, a **new catalog**, so names never collide with legacy tables | silver populated for every migrated group |
| **3. Prove parity** | Compare legacy vs. framework silver per table: row counts, key sets, column-level hashes on a window | agreed thresholds met for N consecutive days |
| **4. Cut over** | Repoint consumers to the framework tables. Where a consumer cannot change, replace the legacy table with a **view** over the framework table | no reads of the legacy tables (`system.access.table_lineage`) |
| **5. Decommission** | Retire the legacy jobs and tables after the retention window | legacy objects dropped |

Migrate **group by group** (one Qlik task at a time), not table by table. A group is the unit of pipeline,
cadence and ownership.

## Why a new catalog rather than taking over the legacy names

A pipeline cannot adopt an existing table that it did not create. Creating `silver.<same name>` would fail while
the legacy table exists. Building in new layer catalogs (`bronze_<env>`, `silver_<env>`):

- keeps legacy and new side by side for parity;
- lets the framework *own* the schemas (grants: only the deployer creates or modifies);
- makes the undeclared-objects audit meaningful from day one.

## Cutover patterns

- **Repoint**: change the consumer's table reference. Preferred.
- **Compatibility view**: `CREATE OR REPLACE VIEW <legacy name> AS SELECT … FROM silver_<env>.facets.<t>`.
  Do this after the legacy table is renamed or dropped. The view is owned outside the framework schemas.
- **Freeze then drop**: revoke writes on the legacy table, keep it read-only through the retention window, then
  drop it.
