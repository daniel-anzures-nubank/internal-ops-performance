# Ajustes Index

Source tab: `Ajustes Index`

Status: partially implemented as explicit hardcoded carve-outs.

Implemented approved rows:

- `nitza.zarza`, component `NO (Normalized Occupancy)`, `2026-04-01` to `2026-05-31`.
  - `metrics/normalized_occupancy.py` suppresses her standalone NO metric output for the affected dates after benchmark construction, so her slots still contribute to the district/shift benchmark.
  - `metrics/xpeer_index.py` removes NO from her Xpeer Index numerator and divisor for Apr-May 2026, matching the legacy recomputation with the remaining components.
- `david.fernandez`, component `Improved Benchmarks`, `2026-04-01` to `2026-04-30`.
  - `metrics/xforce_index.py` removes Improved Benchmarks from the XForce Index numerator and divisor for his XForces in Apr 2026 if that component is present.
  - The standalone `improved_benchmarks` metric is not changed.

This is intentionally not a scalable rules engine. If new rows are added to this tab, add the corresponding explicit code path and tests.
