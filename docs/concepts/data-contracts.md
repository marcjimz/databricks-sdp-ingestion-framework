# Data contracts (ODCS v3.2.0)

Every table has exactly one contract, `tables/<table>/contract.odcs.yaml`, next to its `table.yml`. The standard is the
[Open Data Contract Standard](https://bitol-io.github.io/open-data-contract-standard/) **v3.2.0** (Bitol / LF AI &
Data). The JSON schema is vendored at `contracts/odcs-json-schema-v3.2.0.json`, so lint runs offline.

## What the framework reads from a contract

| Contract element | Used for |
|---|---|
| `schema[0].properties[]` (`name`, `physicalType`, `logicalType`) | bronze reader schema, silver projection + casts |
| `primaryKey` / `primaryKeyPosition` | silver AUTO CDC keys (order matters for composite keys) |
| `required: true` (and every key column) | `<col> IS NOT NULL` expectation |
| `quality[]` with `type: custom`, `engine: databricks-sdp` | SDP expectation; `implementation` is a SQL boolean, `${column}` = the property |
| `customProperties[action]` on a rule | `warn` (default) or `fail` |
| `customProperties[dataClassification]` | registry + table classification |
| `id`, `version` | table property `ingest.contract`, registry, `make trace` |
| `examples` on a property | synthetic data generator (sandbox) |

## House profile (beyond the ODCS minimum)

Lint (`make lint`, pre-commit, tests) rejects a contract unless all of these hold:

- it validates against the ODCS v3.2.0 JSON schema, with `apiVersion: v3.2.0`;
- it has `status`, `team` (owner), `slaProperties` and `customProperties.dataClassification`;
- it has exactly one schema object, whose `physicalName` equals `table` in `table.yml`;
- CDC tables declare a primary key, or set `append_only: true` in `table.yml`;
- every quality rule is enforceable. `type: text` is documentation. `library`/`sql` rules and other engines are
  errors until supported, so nothing *looks* enforced without being enforced;
- no `drop` actions.

## Example

```yaml
schema:
  - name: cmc_cdml_cl_line
    physicalName: CMC_CDML_CL_LINE
    properties:
      - name: CLCL_ID
        logicalType: string
        physicalType: string
        primaryKey: true
        primaryKeyPosition: 1
        required: true
      - name: CDML_SEQ_NO
        logicalType: integer
        physicalType: int
        primaryKey: true
        primaryKeyPosition: 2
        quality:
          - type: custom
            engine: databricks-sdp
            name: claim_line_seq_positive
            implementation: ${column} > 0
            customProperties: [{property: action, value: fail}]   # stop the update
```

## Lifecycle

1. `make scaffold` generates the contract from a data dictionary or Qlik `.dfm` files (columns, types, keys).
2. The data owner adds quality rules, an SLA and a classification, then sets `status: active`.
3. A schema change in the source arrives in bronze `_rescued_data` first. Re-scaffold or edit the contract,
   bump `version`, and the change flows to silver.
