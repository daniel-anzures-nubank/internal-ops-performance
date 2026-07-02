# xpeers_in_target

The **Xpeers In Target** metric (legacy `xpeers_in_target_xforce` /
`xpeers_in_target_xplead`), at two roll-up grains in one table, each at
**day / week / month / quarter / semester / year**:

- `xpeers_in_target` — **XForce** grain, one row per `(team, xforce, xplead)`.
- `xpeers_in_target_xplead` — **XPLead** grain, one row per `(team, xplead)`
  (`xforce` NULL); the same in-target/total counts aggregated to the XPLead.

> `Xpeers In Target = Σ targets achieved / Σ targets * 100`. **Target ≥ 70%.**

A *target* is one agent-metric. For each era-active component, every Xpeer with
that metric contributes one target (denominator) and an achieved target
(numerator) when their value clears the threshold.

Applies to **Core / Fraud / Social Media / Content**. Content was added by the
2026-06-30 legacy re-export (the Content Temp Fix notebook now builds
`xpeers_in_target_*` — before that the Content deck had none). See
`docs/metrics_definitions.md`.

- Module: `metrics/xpeers_in_target.py`
  (`compute_xpeers_in_target` + `compute_xpeers_in_target_xplead`)
- Build script: `scripts/metrics_scripts/build_xpeers_in_target.py` (writes both grains)
- Inputs: the agent-level `io_*_metric` tables (adherence is the driver)
- Default target table: `usr.danielanzures.io_xpeers_in_target_metric`

## Inputs (finished agent-level metric tables, not a raw table)

`io_adherence_metric` (driver — defines the XForce universe and `xplead`),
`io_ntpj_metric`, `io_normalized_occupancy_metric`, `io_quality_metric`,
`io_tnps_metric`, `io_wows_metric`, `io_content_csat_metric` (Content's
Quality component).

## Component targets

| component | threshold | teams |
|-----------|-----------|-------|
| adherence | `>= 95` | all teams |
| ntpj | `<= 100` | Core / Fraud (duration, lower-better) |
| ntpj (SLA) | `>= 95` | Content (SLA compliance, higher-better) |
| normalized_occupancy | `>= 100` | Core / Fraud / Social Media / Content |
| quality | `>= 95` | Core / Fraud / Social Media |
| content_csat | `>= 95` | Content |
| tnps | `>= 88` | Social Media |
| wows | `>= 5` (count) | Social Media |

An agent counts toward a component's **denominator** when they have a row for
that metric in the bucket; toward the **numerator** only when `metric_value`
clears the threshold (NULL `metric_value` fails the target but still counts in
the denominator).

**Content NTPJ direction + the XPLead quirk.** Content NTPJ is the SLA-weighted
compliance metric (bounded ≤ 100, higher-is-better), so its in-target rule is
`>= 95` (Content Temp Fix `ntpj_sla_old_xforces_monthly`, L2301). Legacy's
Content **XPLead** roll-up flags `>= 100` instead
(`ntpj_sla_old_xpleads_monthly`, L2341 — almost certainly a copy-paste of the
NOcc threshold; it makes only perfect-SLA agents count, e.g. March 2026 is
0/17 in-target at the XPLead where the XForce level had 11 in-target of 16).
Per the parity contract we reproduce `>= 100` on the Content XPLead grain for
`date_reference < 2026-07-01` and fix it to `>= 95` from the cutover.

### 2026-06-30 legacy re-export — on-target rule UNCHANGED here

The production re-export of 2026-06-30 introduced a **"90-100 rescale with a
70-cliff"**, but it does **not** touch this metric's on-target flagging. Verified
against the refreshed legacy notebooks (`git diff` of the re-export commit — the
`xpeers_in_target_*` views are byte-identical before/after):

- The per-component in-target thresholds above are the CURRENT legacy rule,
  unchanged: main deck `legacy/[IO] Performance 2026.sql` — adherence `>= 95`
  (L279), ntpj `<= 100` (L570), nocc `>= 100` (L867), qa `>= 95` (L1160);
  SM deck `legacy/[IO] Performance 2026 - Social Media Temp Fix.sql` —
  adherence `>= 95` (L690), nocc `>= 100` (L1776), tnps `>= 88` (L2397),
  wows `>= 5` (L2785), qa `>= 95` (L3271).
- The rescale lives **only** in the legacy `index_xforces_final` views — i.e. in
  how **`xforce_index` consumes** the `xpeers_in_target_xforce` value
  (`>= 70 → 90 + (x - 70) * 10/30`, `< 70 → raw`; main deck L2213-2221, SM deck
  L5559-5568, Content deck likewise). It is ported in
  `metrics/xforce_index.py::_xpeers_in_target_component`.
- Legacy **publishes `xpeers_in_target_xforce` / `xpeers_in_target_xplead`
  unrescaled** (the final export unions `xpeers_in_target_xforces_join` /
  `xpeers_in_target_xpleads_join`, main deck L2531/L2535), so rescaling the
  metric here would diverge from the published table *and* double-apply the
  rescale downstream in `xforce_index`.

## Era windows (anchored on the bucket's month)

- **Core / Fraud**: adherence + ntpj always; `+ quality` from **Feb 2026**;
  `+ normalized_occupancy` from **March 2026**.
- **Social Media**: adherence + tnps + wows always; `+ quality` from **Feb
  2026**; `+ normalized_occupancy` from **March 2026**.
- **Content**: rows exist only from **Feb 2026** — the legacy Content deck's
  save filter (`date_reference < '2026-01-01' OR date_reference >=
  '2026-02-01'`) permanently drops January 2026. Adherence + content_csat from
  the start (CSAT is presence-driven: surveys arrive in monthly batches, so
  weekly CSAT buckets exist only on a handful of Mondays — 2026-02-23, 03-30,
  04-27, 06-01 so far); `+ ntpj (SLA)` and `+ normalized_occupancy` from
  **March 2026** (data-driven in legacy — its Content SLA-NTPJ/NOcc rows start
  then; gated explicitly here because our base tables carry earlier rows, e.g.
  Feb NOcc). The Content **XPLead grain is month-only** pre-cutover (the legacy
  XPLead base excludes `week`, Content Temp Fix L6906).

`day` / `week` / `month` buckets use their own month; `quarter` / `semester` /
`year` anchor on the period's **end** month. Buckets ending before 2026 are
dropped.

### Not reproduced: the legacy 2025-12-01 stray rows

The legacy Content table carries six `2025-12-01` xpeers rows (one
adherence-driver agent, `karina.gonzalez`'s XForce, bucketed into Dec-2025 by an
upstream bad date; the save filter's `date_reference < '2026-01-01'` branch
keeps them). That is an upstream data artifact, not a rule — our adherence table
has no Dec-2025 Content rows and the module's 2026 era floor would drop them
anyway. Deliberately **not** reproduced.

## Output convention

- `numerator` = targets achieved (sum of in-target agent counts across active
  components),
- `denominator` = total targets (sum of agent counts across active components),
- `metric_value` = `numerator / denominator * 100` (NULL when denominator 0).

**Exception — the Content squad/district roll-ups** (see below): `numerator` =
`SUM(metric_value)` of the grain rows, `denominator` = the constant 0 (legacy
`COUNT(DISTINCT agent)` over all-NULL agents), `metric_value` =
`AVG(metric_value)` — a non-NULL average on a denominator-0 row, reproduced
as-is from the legacy Content notebook (L5641-5730 / L7029-7098). The SM
roll-ups instead **sum** the in-target/total counts and divide (each deck
matches its own legacy notebook).

## Deferred to the future Adjustments layer (NOT applied here)

Per-agent carve-outs inherited from the underlying component metrics.

## Output schema (one row per XForce/XPLead per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | always NULL |
| `xforce` | STRING | the XForce (NULL on `xpeers_in_target_xplead` and roll-up rows) |
| `xplead` | STRING | the XPLead (from the Adherence driver; NULL on roll-up rows) |
| `team` | STRING | always NULL (legacy carries none; deck merges Core+Fraud) |
| `squad` | STRING | always NULL |
| `district` | STRING | always NULL |
| `shift` | STRING | always NULL |
| `date_reference` | DATE | bucket start |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | `xpeers_in_target` (XForce), `xpeers_in_target_xplead` (XPLead), or the SM/Content degenerate roll-ups `xpeers_in_target_squad` / `_district` / `_xplead_squad` / `_xplead_district` |
| `numerator` | DOUBLE | targets achieved (Content roll-ups: Σ metric_value) |
| `denominator` | DOUBLE | total targets (Content roll-ups: constant 0) |
| `metric_value` | DOUBLE | `numerator / denominator * 100` (Content roll-ups: AVG of the grain rows) |
