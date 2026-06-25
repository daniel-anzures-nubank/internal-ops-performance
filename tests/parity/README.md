# Parity tests

Diagnostic scripts that compare the new Python pipelines against the
legacy SQL tables they replace.

These are **not** unit tests — they require a live Databricks connection,
take minutes to run, and produce a human-readable report (not pass/fail).
Use them after a substantive change to a metric module to confirm the
output still aligns with the legacy table on real data.

## Running

```bash
# Adherence parity check for an arbitrary period
uv run python tests/parity/parity_check_adherence.py \
    --period-start 2026-04-14 --period-end 2026-04-20

# Dump per-(agent, date) diffs to a CSV for spreadsheet drill-down
uv run python tests/parity/parity_check_adherence.py \
    --period-start 2026-04-14 --period-end 2026-04-20 \
    --csv-out /tmp/adherence_diff.csv

# Compare against legacy AFTER replicating the legacy phantom-adherence bug
# (each unmatched slot gets +1800s added to the new delivered_hours, mimicking
# the legacy LEAST/GREATEST NULL handling). Use this to isolate *other* sources
# of divergence — manual adjustments, table staleness, upstream drift.
uv run python tests/parity/parity_check_adherence.py \
    --period-start 2026-04-14 --period-end 2026-04-20 \
    --replicate-legacy-bug
```

## What "parity" means here

The new pipelines deliberately omit the legacy's manual adjustments
(outage-date exclusions, agent-incident carve-outs, activity-type
reclassifications). Those land in a separate adjustments layer later, so
parity reports show two layers of divergence:

* **Structural divergence** — same logic, same data, numbers should match
  to the second. Any mismatch here is a bug.
* **Adjustment divergence** — the legacy excluded or rewrote slots we
  faithfully include. Expected, and called out explicitly in each
  metric's "Expected divergences" section.
* **Legacy-bug divergence** — defects in the legacy SQL we have *no
  intention* of reproducing (e.g. the phantom-adherence
  `LEAST`/`GREATEST` NULL bug). `--replicate-legacy-bug` simulates these
  inside the parity tool so you can audit what else differs.

The parity report's job is to make the first category obvious by showing
the others separately. If the report says "all divergences land on known
adjustment dates", the pipeline matches the legacy. If it says "you have
a 30-second delta on jane.doe on a random Tuesday", that's a bug.

## Available reports

| Script | Compares against | Period default |
|---|---|---|
| `parity_check_adherence.py` | `usr.mx__cx.adherence_io` | none (must specify) |
