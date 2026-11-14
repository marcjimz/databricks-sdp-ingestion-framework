# Environments and cadence

Environments differ **only in configuration** (`conf/environments/<env>.yml`); the code is identical.

| | sandbox | dev | tst | prod |
|---|---|---|---|---|
| Purpose | prove the framework with synthetic data | shared development | integration | live operational data |
| Default cadence | `on_arrival` | `weekly_sample` | `weekly_sample` | `continuous` |
| Deployer | anyone (SP recommended) | pinned SP | pinned SP | pinned SP |
| `prevent_destroy` | no | no | no | **yes** (pipelines, schemas) |
| Bronze full refresh | allowed | allowed | allowed | **blocked** (`pipelines.reset.allowed=false`) |
| Seed job | yes | never | never | never |
| `make destroy` | yes | yes | yes | **refused** |

## Cadence profiles (`conf/framework.yml`)

| Profile | Mode | Behaviour |
|---|---|---|
| `continuous` | pipeline `continuous: true` | always on; lowest latency; highest cost |
| `on_arrival` | job with file-arrival trigger on the group's landing folder | fires within ~1 min of new Qlik files (min 60 s between runs); cost lever for prod |
| `every_15_minutes` | job with periodic trigger | for sources without a landing folder |
| `weekly_sample` | job with cron (Mon 06:00 UTC), **no full refresh** | dev/tst: baseline read once, then only change files modified in the last day |

Override per group:

```yaml
cadence:
  default: continuous
  overrides:
    facets/provider: on_arrival      # slow-moving reference data: save the always-on cost
```

### Why weekly and not monthly in dev/tst

Ingestion is event-driven: Auto Loader uses **managed file events**. The file-events cache expires after
**7 days** without a run. After that, Auto Loader silently falls back to a full directory listing. Lint and the
enforcement tests therefore reject any triggered cadence that can idle for more than 7 days.

To sample a month, keep the weekly run and widen `ct_lookback_days`. Cost is controlled through `scope`.

### Limiting what non-prod ingests

```yaml
scope:
  groups: [facets/membership, facets/claims]   # only these pipelines
  tables: [facets/claims/CMC_CLCL_CLAIM]       # optionally narrow further
```

## Latency

The Qlik ADLS endpoint batches changes, currently every ~30 minutes. When it moves to ~1 minute:

- `continuous` already processes files as they land;
- `on_arrival` trails by about one to two minutes;
- nothing in the framework changes.
