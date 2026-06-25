# xforce_index

The composite **XForce Index** (legacy `index_xforce`; renamed `xforce_index`
to match the `xpeer_index` convention) — the headline XForce score. One row per
`(team, xforce, xplead)` per **day / week / month / quarter / semester / year**,
for **all four teams**.

> `xforce_index = (shrinkage + xpeers_in_target + average_xpeer_index [+ improved_benchmark]) / N`

It is the **mean of up to four 0–100 normalized components**.

- Module: `metrics/xforce_index.py`
- Build script: `scripts/metrics_scripts/build_xforce_index.py`
- Inputs: `io_shrinkage_metric` (driver), `io_xpeers_in_target_metric`,
  `io_average_xpeer_index_metric`, `io_improved_benchmarks_metric`
- Default target table: `usr.danielanzures.io_xforce_index_metric`

## Components

| component | transform | source |
|-----------|-----------|--------|
| **shrinkage** | `≤20 → 100`; `>20 → 120 − shrinkage`; NULL → 0 | `io_shrinkage_metric` (agent rows **summed** to the XForce → slot-weighted shrinkage %) |
| **xpeers_in_target** | raw value; NULL → 0 | `io_xpeers_in_target_metric` |
| **average_xpeer_index** | raw value; NULL → 0 | `io_average_xpeer_index_metric` |
| **improved_benchmark** | `≥60 → 100`; `<60 → improved / 0.6`; NULL → 0 | `io_improved_benchmarks_metric` (`improved_benchmark_xforce` rows) |

`shrinkage_xforce` is **slot-weighted**: we sum the agent shrinkage
`numerator` / `denominator` per XForce (identical to legacy
`SUM(shrinkage_slot)/SUM(required_slot)`), not an average of agent percentages.

## The improved_benchmark component (Core / Fraud era logic)

The improved_benchmark component is added **iff a matching
`improved_benchmark_xforce` row exists** for the bucket. Because
`improved_benchmarks` is Core/Fraud-only, month-only, and already suppressed
after each team's cutover, this presence test encodes all the rules with no
extra date logic:

- **Core:** 4 components for **month** buckets Jan–Mar 2026; 3 thereafter.
- **Fraud:** 4 components for **month** buckets Jan–Apr 2026; 3 thereafter.
- **Social Media:** always **3 components** (improved_benchmark never applied).
- **Content:** always **3 components** (improved_benchmark excluded — legacy
  added it from April 2026, which we intentionally drop per the business rule).
- **Non-month granularities** (day / week / quarter / semester / year): always
  **3 components** — Improved Benchmarks is month-grain only.

> This deviates from the legacy SM/Content notebooks (which add an
> improved_benchmark component from Mar/Apr 2026). Per the product rule
> "improved benchmarks never applied to SM, and is excluded from April 2026 for
> Content", SM/Content are kept at the 3-component mean.

## Output convention

- `numerator` = Σ active components (each 0–100),
- `denominator` = `100 × N` (300 for 3 components, 400 for 4),
- `metric_value` = `numerator / denominator * 100` (the component mean, 0–100).

## Output schema (one row per XForce per period)

| column | type | notes |
|--------|------|-------|
| `agent` | STRING | always NULL |
| `xforce` | STRING | the XForce |
| `xplead` | STRING | the XForce's XPLead |
| `team` | STRING | `core` / `fraud` / `social media` / `content` |
| `squad` | STRING | always NULL |
| `district` | STRING | always NULL |
| `shift` | STRING | always NULL |
| `date_reference` | DATE | bucket start |
| `date_granularity` | STRING | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | STRING | always `xforce_index` |
| `numerator` | DOUBLE | Σ active components |
| `denominator` | DOUBLE | `100 × N` (300 or 400) |
| `metric_value` | DOUBLE | component mean (0–100) |
