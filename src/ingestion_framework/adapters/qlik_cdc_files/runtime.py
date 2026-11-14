"""Build the SDP graph for one Qlik pipeline group.

Per source table:
  bronze  <bronze>.<source>.<table>   streaming table, append-only history, two append flows
                                      (<t>__full_load from <TABLE>/, <t>__changes from <TABLE>__ct/)
  view    <t>__silver_source           bronze minus before-images, cast to contract types,
                                      carrying the contract's row-level expectations
  silver  <silver>.<source>.<table>   streaming table maintained by AUTO CDC (SCD1/SCD2),
                                      or an append flow for append-only tables

Each table is registered through its own function call so the decorated closures bind that
table's values (no late-binding bugs when looping over hundreds of tables).
"""

from __future__ import annotations

from typing import Any

from ingestion_framework.adapters.base import RuntimeOptions
from ingestion_framework.adapters.qlik_cdc_files import builders as b
from ingestion_framework.config import GroupConfig, ResolvedConfig, TableConfig


def _temporary_view(dp: Any) -> Any:
    # `temporary_view` is the current name; `view` is the legacy alias on older runtimes.
    return getattr(dp, "temporary_view", None) or dp.view


def _table_properties(cfg: ResolvedConfig, t: TableConfig, layer: str) -> dict[str, str]:
    props = {str(k): str(v) for k, v in (cfg.framework.layer(layer).get("table_properties") or {}).items()}
    props["ingest.pipeline"] = cfg.pipeline_name(t.source, t.group)
    props["ingest.contract"] = f"{t.contract.id}@{t.contract.version}"
    if layer == "bronze" and not cfg.env.pipelines.get("bronze_reset_allowed", True):
        props["pipelines.reset.allowed"] = "false"
    return props


def _comment(t: TableConfig, layer: str) -> str:
    base = t.contract.description or t.table
    return f"{base} [{layer}; source {t.source}.{t.table}; contract {t.contract.id}@{t.contract.version}]"


def _define_bronze(
    dp: Any, spark: Any, cfg: ResolvedConfig, group: GroupConfig, t: TableConfig, opts: RuntimeOptions
) -> str:
    qlik = cfg.source(t.source).settings["qlik"]
    landing = cfg.landing_root(t.source)
    target = cfg.table_fqn("bronze", t.source, t.name)
    columns = t.contract.columns

    dp.create_streaming_table(
        name=target,
        comment=_comment(t, "bronze"),
        table_properties=_table_properties(cfg, t, "bronze"),
    )

    full_path = b.full_load_path(landing, group.landing_folder, t.table)
    full_schema = b.reader_schema_ddl(columns, qlik, include_headers=False)
    full_select = b.bronze_select_full_load(columns, qlik)
    full_opts = b.reader_options(qlik, allow_overwrites=True)  # a reload may reuse LOAD00000001

    @dp.append_flow(target=target, name=f"{t.name}__full_load")
    def full_load() -> Any:
        reader = spark.readStream.format("cloudFiles").options(**full_opts).schema(full_schema)
        return reader.load(full_path).selectExpr(*full_select)

    ct_path = b.changes_path(landing, group.landing_folder, t.table, str(qlik["ct_suffix"]))
    ct_schema = b.reader_schema_ddl(columns, qlik, include_headers=True)
    ct_select = b.bronze_select_changes(columns, qlik)
    ct_opts = b.reader_options(qlik, lookback_days=opts.ct_lookback_days, now=opts.now)

    @dp.append_flow(target=target, name=f"{t.name}__changes")
    def changes() -> Any:
        reader = spark.readStream.format("cloudFiles").options(**ct_opts).schema(ct_schema)
        return reader.load(ct_path).selectExpr(*ct_select)

    return target


def _define_silver(dp: Any, spark: Any, cfg: ResolvedConfig, t: TableConfig, bronze: str, opts: RuntimeOptions) -> None:
    qlik = cfg.source(t.source).settings["qlik"]
    target = cfg.table_fqn("silver", t.source, t.name)
    view_name = f"{t.name}__silver_source"
    cutoff = opts.reload_cutoffs.get(target)
    where = b.silver_source_filter(qlik, reload_cutoff=cutoff)
    projection = b.silver_projection(t.contract.columns, qlik)
    warn = {e.name: e.condition for e in t.contract.expectations if e.action == "warn"}
    fail = {e.name: e.condition for e in t.contract.expectations if e.action == "fail"}

    @_temporary_view(dp)(name=view_name)
    @dp.expect_all(warn)
    @dp.expect_all_or_fail(fail)
    def silver_source() -> Any:
        return spark.readStream.table(bronze).where(where).selectExpr(*projection)

    silver_layer = cfg.framework.layer("silver")
    props = _table_properties(cfg, t, "silver")
    if cutoff:  # which reload this table was rebuilt from: SHOW TBLPROPERTIES answers it
        props["ingest.reload_cutoff"] = cutoff
    dp.create_streaming_table(
        name=target,
        comment=_comment(t, "silver"),
        table_properties=props,
        cluster_by_auto=bool(silver_layer.get("cluster_by_auto", False)),
    )

    if t.append_only:
        append_columns = [c for c in projection if c not in {f"`{x}`" for x in b.silver_except_columns(qlik)}]

        @dp.append_flow(target=target, name=f"{t.name}__append")
        def append() -> Any:
            return spark.readStream.table(view_name).selectExpr(*append_columns)

        return

    dp.create_auto_cdc_flow(
        target=target,
        source=view_name,
        keys=list(t.contract.keys),
        sequence_by=b.CDC_SEQ,
        apply_as_deletes=b.delete_condition(qlik),
        except_column_list=b.silver_except_columns(qlik),
        stored_as_scd_type=t.scd_type,
        name=f"{t.name}__apply_changes",
    )


def define_table(
    dp: Any, spark: Any, cfg: ResolvedConfig, group: GroupConfig, t: TableConfig, opts: RuntimeOptions
) -> None:
    bronze = _define_bronze(dp, spark, cfg, group, t, opts)
    if t.silver:
        _define_silver(dp, spark, cfg, t, bronze, opts)


def define_group(dp: Any, spark: Any, cfg: ResolvedConfig, group: GroupConfig, opts: RuntimeOptions) -> None:
    for t in group.tables:
        define_table(dp, spark, cfg, group, t, opts)
