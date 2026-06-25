# Correcciones Generales Datos

Source tab: `Correcciones Generales Datos`

Status: partially implemented as explicit hardcoded source corrections.

Implemented approved rows:

- `luis.contreras`, source `Taskmaster (jobs OOS)`, `2026-01-01` to `2026-03-08`, correction `Timestamps +2 horas`.
- `luis.contreras`, source `Taskmaster (jobs OOS)`, `2026-03-09` to `2026-05-19`, correction `Timestamps +1 hora`.

Implementation:

- `metrics_data/occupancy_time.py` shifts the affected Content Taskmaster/OOS job `local_start_date`, `local_stop_date`, `activity_start_unix`, `activity_end_unix`, and derived `date` before NOCC slot-overlap math.
- The correction is scoped to Content OOS jobs for `luis.contreras`.
- `net_time_spent_seconds` is not changed; only the job interval placement changes.

This is intentionally not a scalable rules engine. If new corrections are added to this tab, add explicit code and tests for each correction.
