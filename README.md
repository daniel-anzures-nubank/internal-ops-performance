# Internal Ops Performance

Internal Operations (IO) performance metrics for **Nubank Mexico CX** — agent,
XForce, and XPLead metrics (Adherence, NTPJ, Normalized Occupancy, Quality,
Shrinkage, tNPS, WoWs, Xpeer/XForce Index, …) for the **Core, Fraud, Social
Media, and Content** teams.

The pipeline is a PySpark rebuild of the legacy Databricks SQL notebooks in
`legacy/`, which remain the **source of truth for metric definitions** until
each metric is migrated (outputs for dates before `2026-07-01` are held at
byte-for-byte parity with legacy).

## Architecture

Four layers — the Python ones pure `pyspark.sql` (DataFrame-in / DataFrame-out),
with transport isolated in `db.py` and the `scripts/` entry points:

1. `extractors/` — parameterized SQL pulls from the upstream Databricks tables
   (no filters, no business logic).
2. `metrics_data/` — the **raw data tables** (per slot / job / evaluation),
   minimal filtering only.
3. `metrics/` — the **finished metrics** (business exclusions, benchmarks,
   ratios), emitted in a tidy long format at day / week / month / quarter /
   semester / year grain.
4. `adjustments/` — **manual adjustments** (per-agent / per-date carve-outs),
   synced from the adjustments Google Sheet and applied inside the metric layer.

Each layer has a matching `scripts/*_scripts/build_*.py` entry point and a doc
folder under `docs/`.

### Table naming

All output lives in the `usr.danielanzures` schema:

- raw tables are suffixed `_raw` (e.g. `io_adherent_time_raw`),
- metric tables are suffixed `_metric` (e.g. `io_adherence_metric`),
- every table has an append-only history twin suffixed `_snapshots`, and each
  build is recorded in the `pipeline_runs` registry (see `db.publish()`).

## Running on Databricks

The `[IO] Performance Metrics Pipeline` job runs **27 git-sourced tasks** (an
adjustments-sheet sync feeding the raw-table and metric builds; `spark_python_task`,
`source: GIT`) that check out GitHub `main` fresh on every run — push to `main` to change what the pipeline does. The workspace git folder
(`/Workspace/Users/daniel.anzures@nubank.com.mx/internal-ops-performance`) is a
separate, manually refreshed checkout for browsing in the UI; the job never
reads it.

## Tests

PySpark unit tests against a local SparkSession (no warehouse). PySpark needs a
Python ≤ 3.12 interpreter:

```bash
export PYSPARK_PYTHON=$PWD/.venv/bin/python
.venv/bin/python -m pytest -q
```

## Documentation

- `CLAUDE.md` — the full project guide (layers, SOT tables, conventions,
  deployment).
- `docs/metrics_definitions.md` — canonical metric definitions and formulas.
- `docs/metrics_docs/` — one doc per finished metric.
- `docs/metrics_data_docs/` — one doc per raw table.
