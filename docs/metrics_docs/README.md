# metrics — finished metric documentation

The modules in `metrics/` turn the raw `io_*_raw` tables (built by
`metrics_data/`) into **finished performance metrics**. This is where the
business exclusions, benchmarks, ratios, and (eventually) manual adjustments the
raw layer deferred get applied.

## Shape (shared across all metric tables)

Every metric table is tidy "long" format, one row per agent per period:

| column | notes |
|--------|-------|
| `agent`, `xforce`, `xplead`, `team`, `squad`, `district`, `shift` | roster dimensions; hierarchy/dimension fields take their **most-recent value within the period bucket** (legacy `FIRST_VALUE(... ORDER BY date DESC)`) |
| `date_reference` | bucket start: the day, the **Monday** of the week (Spark `DATE_TRUNC('WEEK')`), the first of the month/quarter/year, or Jan 1 / Jul 1 for a semester |
| `date_granularity` | `day` / `week` / `month` / `quarter` / `semester` / `year` |
| `metric` | the metric name (e.g. `adherence`) |
| `numerator`, `denominator` | the raw components (units are metric-specific) |
| `metric_value` | **percentage** = `numerator / denominator * 100`; NULL when denominator is 0 |

## Naming convention

Raw `metrics_data` tables are suffixed `_raw`; metric tables are suffixed
`_metric`. Both live in `usr.danielanzures`.

## Metrics

| doc | module | build script | input raw table | default target |
|-----|--------|--------------|-----------------|----------------|
| [adherence](adherence.md) | `metrics/adherence.py` | `scripts/metrics_scripts/build_adherence.py` | `io_adherent_time_raw` | `usr.danielanzures.io_adherence_metric` |
| [ntpj](ntpj.md) | `metrics/ntpj.py` | `scripts/metrics_scripts/build_ntpj.py` | `io_jobs_raw` (+ `io_jobs_within_sla_raw` for Content) | `usr.danielanzures.io_ntpj_metric` |
| [content_sla_ntpj](content_sla_ntpj.md) (Content NTPJ) | `metrics/content_sla_ntpj.py` | `scripts/metrics_scripts/build_ntpj.py` (unioned in place of the Content duration rows) | `io_jobs_within_sla_raw` | `usr.danielanzures.io_ntpj_metric` (shared, `metric='ntpj'`) |
| [ntpj_xforce](ntpj_xforce.md) | `metrics/ntpj_xforce.py` | `scripts/metrics_scripts/build_ntpj_xforce.py` | `io_ntpj_metric` (not a raw table) | `usr.danielanzures.io_ntpj_xforce_metric` |
| [normalized_time_per_job](normalized_time_per_job.md) | `metrics/ntpj.py` (`compute_normalized_time_per_job`) | `scripts/metrics_scripts/build_normalized_time_per_job.py` | `io_jobs_raw` | `usr.danielanzures.io_normalized_time_per_job` |
| [normalized_occupancy](normalized_occupancy.md) | `metrics/normalized_occupancy.py` | `scripts/metrics_scripts/build_normalized_occupancy.py` | `io_occupancy_time_raw` | `usr.danielanzures.io_normalized_occupancy_metric` |
| [quality](quality.md) | `metrics/quality.py` | `scripts/metrics_scripts/build_quality.py` | `io_quality_evaluations_raw` | `usr.danielanzures.io_quality_metric` |
| [shrinkage](shrinkage.md) | `metrics/shrinkage.py` | `scripts/metrics_scripts/build_shrinkage.py` | `io_shrinkage_slots_raw` | `usr.danielanzures.io_shrinkage_metric` |
| [tnps](tnps.md) | `metrics/tnps.py` | `scripts/metrics_scripts/build_tnps.py` | `io_tnps_responses_raw` | `usr.danielanzures.io_tnps_metric` |
| [wows](wows.md) | `metrics/wows_metric.py` | `scripts/metrics_scripts/build_wows.py` | `io_wows_raw` | `usr.danielanzures.io_wows_metric` |
| [content_csat](content_csat.md) | `metrics/content_csat_metric.py` | `scripts/metrics_scripts/build_content_csat.py` | `io_content_csat_raw` | `usr.danielanzures.io_content_csat_metric` |
| [improved_benchmarks](improved_benchmarks.md) | `metrics/improved_benchmarks.py` | `scripts/metrics_scripts/build_improved_benchmarks.py` | `io_normalized_time_per_job` + `io_occupancy_time_raw` + `io_ntpj_xforce_metric` | `usr.danielanzures.io_improved_benchmarks_metric` |
| [xpeer_index](xpeer_index.md) | `metrics/xpeer_index.py` | `scripts/metrics_scripts/build_xpeer_index.py` | the seven `io_*_metric` tables (not a raw table) | `usr.danielanzures.io_xpeer_index_metric` |
| [nuvinhos_performance](nuvinhos_performance.md) | `metrics/nuvinhos_performance.py` | `scripts/metrics_scripts/build_nuvinhos_performance.py` | `io_xpeer_index_metric` + `agent_information` | `usr.danielanzures.io_nuvinhos_performance_metric` |
| [xpeers_in_target](xpeers_in_target.md) | `metrics/xpeers_in_target.py` | `scripts/metrics_scripts/build_xpeers_in_target.py` | the agent-level `io_*_metric` tables (not a raw table) | `usr.danielanzures.io_xpeers_in_target_metric` |
| [average_xpeer_index](average_xpeer_index.md) | `metrics/average_xpeer_index.py` | `scripts/metrics_scripts/build_average_xpeer_index.py` | `io_xpeer_index_metric` | `usr.danielanzures.io_average_xpeer_index_metric` |
| [xforce_index](xforce_index.md) | `metrics/xforce_index.py` | `scripts/metrics_scripts/build_xforce_index.py` | `io_shrinkage_metric` + `io_xpeers_in_target_metric` + `io_average_xpeer_index_metric` + `io_improved_benchmarks_metric` | `usr.danielanzures.io_xforce_index_metric` |
| [average_xforce_index](average_xforce_index.md) | `metrics/average_xforce_index.py` | `scripts/metrics_scripts/build_average_xforce_index.py` | `io_xforce_index_metric` | `usr.danielanzures.io_average_xforce_index_metric` |

Shared aggregation (bucketing + the tidy long output) lives in
`metrics/metric_utils.py`.

> **Notes / exceptions to the shared shape above:**
> - `ntpj` is **team-split**: Core/Fraud is the duration ratio (lower-is-better,
>   target ≤ 100); Content is the SLA-weighted compliance from
>   [content_sla_ntpj](content_sla_ntpj.md) (higher-is-better, target ≥ 95),
>   unioned into the same `io_ntpj_metric` table by `build_ntpj.py`.
> - `ntpj_xforce` (no agent grain): the XForce roll-up of `ntpj`, **week + month
>   only**, with a team-aware on-target rule (Core/Fraud ≤ 100, Content ≥ 95).
> - `normalized_time_per_job` is **not a tidy metric** — the NTPJ benchmark
>   substrate (per agent × job_id × month, `exp_duration_job`) that
>   `improved_benchmarks` consumes. No `_metric` suffix on its table.
> - `improved_benchmarks` emits `improved_benchmark_xforce` only — **XForce
>   grain** (not agent), **month-only**, and **Core/Fraud only**.
> - `wows` is a **count** metric: `metric_value` is the WoW count (not
>   `numerator / denominator * 100`); `denominator` just carries the target (5).
> - `tnps` keeps the ratio shape but `metric_value` is an NPS % that can be
>   **negative**.
> - `xpeer_index` is a **composite**: it reads the other `io_*_metric` tables
>   (not an `io_*_raw` table) and folds them into a single mean. Its component
>   roster is **team- and era-dependent** (NO from March 2026, Quality from Feb,
>   etc.); multi-month buckets anchor the era on the period's **end** month.
> - `nuvinhos_performance` is **index-level** (no agent grain): it reads
>   `io_xpeer_index_metric` + `agent_information` tenure and emits three
>   roll-ups (XForce / squad / district) comparing new vs tenured agents. It uses
>   the documented flat `avg(Index|Nuvinho) / avg(Index|old)` (not legacy's
>   cohort-count-biased two-level average).
> - `xpeers_in_target` (no agent grain): it reads the agent-level `io_*_metric`
>   tables, flags each agent in/out of target per component (adherence ≥95, ntpj
>   ≤100, NO ≥100, quality ≥95, tnps ≥88, wows ≥5), and reports targets-achieved
>   / total-targets at two grains in one table — `xpeers_in_target` (**XForce**)
>   and `xpeers_in_target_xplead` (**XPLead**, `xforce` NULL). Core/Fraud +
>   Social Media only; era-gated like the Index.
> - `shrinkage` is **agent-level**, but its build also writes two slot-weighted
>   roll-ups into the same table: `shrinkage_xforce` (per XForce) and
>   `shrinkage_xplead` (per XPLead, `xforce` NULL).
> - `average_xpeer_index` is **XForce-level** (no agent grain): the simple mean
>   of the agent-level Xpeer Index per XForce, all four teams. (`numerator` =
>   Σ index, `denominator` = agent count, vs legacy's NULL/`AVG`.)
> - `xforce_index` is **XForce-level** (no agent grain): the composite headline
>   score — mean of up to 4 normalized 0–100 components (shrinkage,
>   xpeers_in_target, average_xpeer_index, improved_benchmark). improved_benchmark
>   is added only where an `improved_benchmark_xforce` row exists (Core/Fraud,
>   month, pre-cutover); SM/Content stay 3-component. (Legacy `index_xforce`.)
> - `average_xforce_index` is **XPLead-level** (no agent/xforce grain): the simple
>   mean of `xforce_index` per XPLead, all four teams. (`numerator` = Σ index,
>   `denominator` = XForce count, vs legacy's NULL/`AVG`.)
>
> See each metric's doc for details.
