# Metrics Definitions

> **Source of truth:** [\[IO\] Metrics Repository Details (Google Docs)](https://docs.google.com/document/d/1o-HP58HXOLBI1mtZLA64Y4uB2pci_QbjIXnETGOY9Q8/edit?tab=t.0)
>
> The Google Doc above is the canonical, human-maintained version. This file mirrors it for in-repo and agent access — keep it in sync whenever the doc changes.
>
> The link requires Nubank SSO. Agents can read it via the **Google Workspace MCP** (`docs_getText`, documentId `1o-HP58HXOLBI1mtZLA64Y4uB2pci_QbjIXnETGOY9Q8`); a plain web fetch hits the sign-in wall. The mirror below is kept for convenience.
>
> _Source doc last updated 2026-06-09 · mirror last synced 2026-06-09._

Canonical definitions for every Internal Ops performance metric. Adapted from **[IO] Metrics Repository Details** (Author: Daniel Fernandez, last updated 2026-06-09, RFC Bet 6: Xpeers Individual Performance Score).

This document mirrors the source-of-truth Google Doc for metric formulas, targets, datasets, and edge cases. When refactoring legacy SQL into Python, every metric implementation must trace back to a section here.

**Conventions used below:**

- **Description** — what the metric measures, in plain language.
- **Formula** — the canonical mathematical definition.
- **Target** — the performance threshold (≥ or ≤).
- **Datasets** — source tables required to compute it.
- **Filters & Caveats** — filtering, exclusions, status restrictions, transformations.
- **Example** — worked example reproducing the PDF.
- **Edge Cases / Legacy Quirks** — placeholders to be filled while migrating; document anything that surprises us in legacy SQL here.

**Role hierarchy:**

- **Xpeer** — individual customer-service agent.
- **XForce** — the Xpeer's manager.
- **XPLead** — the XForce's manager (the Xpeer's "manager's manager").

---

## Table of Contents

- [Core & Fraud](#core--fraud)
  - [Xpeer](#xpeer-core--fraud)
  - [XForce](#xforce-core--fraud)
  - [XPLead](#xplead-core--fraud)
- [Social Media](#social-media)
  - [Xpeer](#xpeer-social-media)
  - [XForce](#xforce-social-media)
  - [XPLead](#xplead-social-media)
- [Content](#content)
  - [Xpeer](#xpeer-content)
  - [XForce](#xforce-content)
  - [XPLead](#xplead-content)

---

## Core & Fraud

### Xpeer (Core & Fraud)

#### 1. Adherence

- **Description:** Percentage of time the Xpeer was connected in shuffle out of their dimensioned time in DIME. It overlaps the time spent on the relevant statuses (`available`, `oos`, `training`, `paused_with_jobs`, `active_jobs > 0`) from the `agent_productivity` tables with the DIME slots.
- **Formula:** `Connected Time / Dimensioned Time`
- **Target:** `>= 95%`
- **Datasets:**
  - `agent_productivity`
  - `agent_dimensioned_activities`
  - `staffing_hero__actor_status_status_history`
  - `staffing_hero__actor_statuses`
  - `staffing_hero__status_options`
- **Filters & Caveats:**
  - Only status in `available`, `oos`, `training`, `paused_with_jobs`, or `active_jobs > 0`.
  - Dimensioned activities **not** taken into account: `lunch_break`, `time_off`, `shrinkage`.
  - Connected time = the **overlap** between the productivity status intervals and the DIME slots.
- **Example (Nuberto, 3-day period):**

  | Day | Connected Time (h) | Dimensioned Time (h) |
  | --- | --- | --- |
  | 1 | 6.5 | 8 |
  | 2 | 7 | 7.5 |
  | 3 | 6 | 6 |

  `Adherence = (6.5 + 7 + 6) / (8 + 7.5 + 6) = 90.7%`

- **Edge Cases / Legacy Quirks:** _TBD — fill in during migration._

#### 2. Normalized Time Per Job (NTPJ)

- **Description:** Percentage of time spent on activities compared to the expected time for those activities. The expected-time benchmark is the average time per job for the whole month. Only activities whose type matches the activity type required for DIME are considered.
- **Formula:** `Time Per Job / Expected Time Per Job`
- **Target:** `<= 100%`
- **Datasets:**
  - `ops_canonical_time_spent_activities`
  - `taskmaster_consolidated_registry`
  - `agent_dimensioned_activities`
- **Filters & Caveats:**
  - Only **finished** jobs.
  - Only jobs and time spent on dimensioned activities in DIME, considered across the **whole day**.
  - Only shuffle status in `available`, `oos`.
  - Only activities matching the activity type **required for the day** in DIME — even if it wasn't the type required for that specific slot, it counts if it was required somewhere that day.
  - **Benchmark window:** as of **April 2026** this switched from a trailing **4-month** window to the **current month only**.
- **Example (Nuberto, January):**

  | Job | Jobs Done | Total Time | Avg Time / Job | Benchmark |
  | --- | --- | --- | --- | --- |
  | Account Cancellation | 3 | 15 min | 5 min | 6 min |
  | Account Creation | 5 | 30 min | 6 min | 7 min |

  `NTPJ = (5 + 6) / (6 + 7) = 84.6%`

- **Edge Cases / Legacy Quirks:** _TBD._

#### 3. Normalized Occupancy (NO)

- **Description:** Agent's occupancy divided by the average occupancy for their team and shift for that month. Occupancy is the percentage of dimensioned time the agent was working on jobs (shuffle + out of shuffle). Only activities matching the DIME-required activity type **in the exact slot** are taken into account.
- **Formula:** `Occupancy / Occupancy Benchmark`
- **Target:** `>= 100%`
- **Datasets:**
  - `agent_dimensioned_activities`
  - `ops_canonical_time_spent_activities`
  - `taskmaster_consolidated_registry`
- **Filters & Caveats:**
  - Dimensioned activities **not** taken into account: `lunch_break`, `time_off`, `shrinkage`.
  - Only jobs in `finished`, `transferred`, `skipped`.
  - All jobs out of shuffle are taken into account (no exclusions).
  - Only activities matching the activity type required for DIME **in the exact slot** (contrast with NTPJ, which matches any activity required somewhere that day).
  - Started being measured in **March 2026**.
- **Example (Nuberto, January):**

  | Working Time on Jobs | Dimensioned Time | Avg Squad/Shift Occupancy (Benchmark) |
  | --- | --- | --- |
  | 6.3 h | 7.5 h | 92% |

  `Occupancy = 6.3 / 7.5 = 84%` → `NO = 84% / 92% = 91.3%`

- **Edge Cases / Legacy Quirks:** _TBD._

#### 4. Quality

- **Description:** Average of all quality evaluations performed by the service excellence team for the period.
- **Formula:** `Sum of Quality Evaluations / Total Quality Evaluations`
- **Target:** `>= 95%`
- **Datasets:** `qmo_playvox_consolidated`
- **Filters & Caveats:**
  - Keep only the **latest record per `evaluation_id`**.
- **Example (Nuberto, January):** Evaluations of 95%, 93%, 81% → `(95 + 93 + 81) / 3 = 89.7%`.
- **Edge Cases / Legacy Quirks:** _TBD._

#### 5. Xpeer Index

- **Description:** Weighted index combining the 4 Xpeer performance metrics (Adherence, NTPJ, NO, Quality) into a single metric so agents can be compared across teams/squads.
- **Formula:** `(Adherence + NTPJ + NO + Quality) / 4`
- **Target:** `>= 95%`
- **Datasets:** Union of all datasets used by Adherence, NTPJ, NO, and Quality.
- **Filters & Caveats:**
  - If `NTPJ > 100%`, apply the transformation `100% - (NTPJ - 100%)`.
  - If `NO > 100%`, truncate to `100%`.
  - **NO is only included in the Index from March 2026 onward.**
  - If the Xpeer has no Quality evaluation for the month, they are **not** taken into account.
- **Example A (Nuberto):**

  | Adherence | NTPJ | NO | Quality | Xpeer Index |
  | --- | --- | --- | --- | --- |
  | 90.7% | 84.6% | 91.3% | 89.7% | `(90.7 + 84.6 + 91.3 + 89.7) / 4 = 89.1%` |

- **Example B (Nurissa, with transformations):**

  | Adherence | NTPJ | NO | Quality | Xpeer Index |
  | --- | --- | --- | --- | --- |
  | 99.3% | 107.4% | 113.2% | 95% | `(99.3 + [100 - (107.4 - 100)] + 100 + 95) / 4 = 96.7%` |

- **Edge Cases / Legacy Quirks:** _TBD — confirm pre-March-2026 vs post-March-2026 behaviour against legacy SQL._

---

### XForce (Core & Fraud)

#### 1. Xpeers In Target

- **Description:** Percentage of targets met by the Xpeers managed by the XForce.
- **Formula:** `Total Number of Targets Achieved / Total Number of Targets`
- **Target:** `>= 70%`
- **Datasets:** N/A (computed from Xpeer-level metrics).
- **Filters & Caveats:** N/A.
- **Example (Nuliana, January):**

  | Agent | Adherence | NTPJ | NO | Quality | Targets Achieved |
  | --- | --- | --- | --- | --- | --- |
  | Nuberto | 90.7% | 84.6% | 91.3% | 89.7% | 1 |
  | Nudrigo | 96.4% | 97.2% | 89% | 100% | 3 |
  | Nurissa | 99.3% | 107.4% | 113.2% | 95% | 3 |

  `Xpeers in Target = (1 + 3 + 3) / (4 + 4 + 4) = 58.3%`

- **Edge Cases / Legacy Quirks:** Each Xpeer contributes one target per **era-active** component (a target is achieved when the metric clears its threshold): adherence `≥95`, NTPJ `≤100`, NO `≥100`, Quality `≥95` (Core/Fraud); Social Media swaps NTPJ for tNPS `≥88` and WoWs `≥5`. Quality joins from Feb 2026, NO from March 2026 (so Jan has 2 targets, Feb 3, March+ 4 — or 5 for SM). The denominator is the **sum of agent-targets** (only agents who have each metric), not literally `4 × Xpeers`. A NULL `metric_value` counts in the denominator but never as achieved. **Content has no Xpeers In Target.**

#### 2. Average Xpeer Index

- **Description:** Average Xpeer Index across all Xpeers managed by the XForce.
- **Formula:** `Sum of Xpeer Index / Total Xpeers`
- **Target:** `>= 90%`
- **Datasets:** N/A.
- **Filters & Caveats:** N/A.
- **Example (Nuliana, January):** Nuberto 89.1%, Nudrigo 95.7%, Nurissa 96.7% → `(89.1 + 95.7 + 96.7) / 3 = 93.8%`.
- **Edge Cases / Legacy Quirks:** _TBD._

#### 3. Shrinkage

- **Description:** Percentage of time spent on non-productive activities out of total dimensioned time for the Xpeers managed by the XForce.
- **Formula:** `Non-productive Time / Dimensioned Time`
- **Target:** `<= 20%`
- **Datasets:** `agent_dimensioned_activities`.
- **Filters & Caveats:**
  - `activity_type_required` in `Shrinkage`, OR `dime_invalid_notation` with values: `Mouring`, `Weekly`, `Permiso Medico`, `Permiso medico`, `Huddle`.
- **Example (Nuliana, January):**

  | Agent | Non-productive Hours | Dimensioned Hours |
  | --- | --- | --- |
  | Nuberto | 15 | 120 |
  | Nudrigo | 30 | 120 |
  | Nurissa | 20 | 100 |

  `Shrinkage = (15 + 30 + 20) / (120 + 120 + 100) = 19.1%`

- **Edge Cases / Legacy Quirks:** _TBD — confirm exact spelling of `Mouring` vs `Mourning` against the legacy SQL._

#### 4. Improved Benchmarks

> ⚠️ **Removed entirely — no longer computed or reported.** It was dropped per team: Core from **April 2026**, Fraud from **May 2026**; it **never applied** to Social Media or Content. In the new pipeline (`metrics/improved_benchmarks.py`) it is computed only for Core/Fraud and **suppressed from each team's cutover onward**, so no rows are emitted for those months.

- **Description:** Percentage of Time-Per-Job and Occupancy benchmarks that improved month over month, out of the total.
- **Formula:** `Total Improved Benchmarks / Total Benchmarks`
- **Target:** `>= 60%`
- **Datasets:** N/A.
- **Filters & Caveats:**
  - Benchmarks that **stay the same** are considered **improved**.
- **Example (Nuliana, February):**

  | Job | Jan (s) | Feb (s) | Comparison |
  | --- | --- | --- | --- |
  | Account Creation | 230 | 250 | Improved |
  | Account Cancellation | 190 | 170 | Worsened |
  | Fraud Suspect | 350 | 350 | Stayed the same |

  | District | Shift | Jan | Feb | Comparison |
  | --- | --- | --- | --- | --- |
  | CSI | Morning | 70% | 75% | Improved |
  | CSI | Mid | 50% | 40% | Worsened |
  | Gamers | Mid | 89% | 92% | Improved |
  | Gamers | Night | 78% | 78% | Stayed the same |

  The doc text reads _"improved in 1 out of 3 jobs and in 2 out of 4 occupancy benchmarks"_ and leaves the result blank. Applying the stated "stayed-the-same = improved" rule gives `(2 + 3) / (3 + 4) = 71.4%`.
- **Edge Cases / Legacy Quirks:** _Two unresolved inconsistencies in the source doc (carry into migration): (1) the prose counts only strictly-improved benchmarks (1/3 jobs, 2/4 occ → 42.9%), but its own "stayed-the-same = improved" rule yields 71.4% — reconcile against legacy SQL. (2) The jobs table labels a Time-Per-Job increase (230→250s) as "Improved", which is inverted vs the legacy logic where a **lower** benchmark counts as improved (`benchmark <= previous_benchmark`); the occupancy table (higher = improved) is consistent. Confirm direction per metric. Also: dropped from the Index **starting April 2026 for Core and May 2026 for all teams** (source doc 2026-06-09) — confirm whether Fraud follows the "all teams" (May) date._

#### 5. Nuvinhos Performance

- **Description:** Compares the average Xpeer Index of **Nuvinhos** (agents with less than 2 full months of tenure during the evaluation period) with the average Xpeer Index of older agents.
- **Formula:** `Average Xpeer Index (Nuvinhos) / Average Xpeer Index (Non-Nuvinhos)`
- **Datasets:** N/A.
- **Filters & Caveats:**
  - An agent is considered Nuvinho if, during the evaluation period, they had less than **2 full months** completed.
  - Only the Xpeer Index is used for comparison.
- **Example (Nuliana, January):**

  | Agent | < 2 Full Months | Xpeer Index |
  | --- | --- | --- |
  | Nuberto | Yes | 89% |
  | Nuria | Yes | 84.3% |
  | Nudrigo | No | 95.7% |
  | Nurissa | No | 96.7% |

  `Nuvinhos avg = (89 + 84.3) / 2 = 86.7%`, `Older avg = (95.7 + 96.7) / 2 = 96.2%` → `Nuvinhos Performance = 86.7 / 96.2 = 90.1%`.

- **Edge Cases / Legacy Quirks:** _TBD — clarify "less than 2 full months" with respect to hire-date semantics in `cx_mx_bdx_snapshots`._

#### 6. XForce Index

- **Description:** Weighted index combining the **3** XForce-level metrics (Xpeers In Target, Average Xpeer Index, Shrinkage) into a single metric. **Excludes** Nuvinhos Performance. (Improved Benchmarks was a 4th component until it was **removed from the Index in May 2026**.)
- **Formula:** `(Xpeers In Target + Average Xpeer Index + Shrinkage_transformed) / 3`
- **Datasets:** N/A.
- **Filters & Caveats:**
  - Does **not** include Nuvinhos Performance.
  - **Shrinkage:** if `<= 20%`, truncate to `100%`; if `> 20%`, transform as `120% - Shrinkage`.
  - **Improved Benchmarks removed from the Index — April 2026 for Core, May 2026 for all teams** (previously: truncate to `100%` if `>= 60%`, else transform as `Improved Benchmarks / 60%`).
- **Example A (Nuliana, January):**

  | Xpeers In Target | Avg Xpeer Index | Shrinkage | XForce Index |
  | --- | --- | --- | --- |
  | 58.3% | 93.8% | 19.1% | `(58.3 + 93.8 + 100) / 3 = 84%` |

- **Example B (Nuleo, January):**

  | Xpeers In Target | Avg Xpeer Index | Shrinkage | XForce Index |
  | --- | --- | --- | --- |
  | 71.9% | 86.6% | 28.7% | `(71.9 + 86.6 + [120 - 28.7]) / 3 = 83.3%` |

- **Edge Cases / Legacy Quirks:** _The source doc applies the 3-component formula even to historical (e.g. January) examples. Confirm whether pre-May-2026 XForce Index values should be recomputed with 3 components or keep the original 4-component definition (incl. Improved Benchmarks) for historical months._

---

### XPLead (Core & Fraud)

#### 1. Xpeers In Target

- **Description:** Percentage of targets met by the Xpeers managed by the XPLead.
- **Formula:** `Total Number of Targets Achieved / Total Number of Targets`
- **Target:** `>= 70%`
- **Datasets:** N/A.
- **Filters & Caveats:** N/A.
- **Example (Nuliana, January):**

  | Xpeer | Adherence | NTPJ | NO | Quality | Xpeer Index |
  | --- | --- | --- | --- | --- | --- |
  | Nuberto | 90.7% | 84.6% | 91.3% | 89.7% | 89% |
  | Nudrigo | 96.4% | 97.2% | 89% | 100% | 95.7% |
  | Nurissa | 99.3% | 107.4% | 113.2% | 95% | 96.7% |
  | Nuria | 94% | 103.2% | 98.8% | 100% | 97.4% |
  | Nuricio | 88% | 93.9% | 104.7% | 92% | 93.5% |

  `Average Xpeer Index = (89 + 95.7 + 96.7 + 97.4 + 93.5) / 5 = 94.5%`

- **Edge Cases / Legacy Quirks:** _TBD — the PDF example illustrates the Average Xpeer Index computation rather than the Xpeers-in-Target denominator/numerator. Confirm exact computation against legacy SQL._

#### 2. Average XForce Index

- **Description:** Average XForce Index across all XForces managed by the XPLead.
- **Formula:** `Sum of XForce Index / Total XForces`
- **Target:** `>= 90%`
- **Datasets:** N/A.
- **Filters & Caveats:** N/A.
- **Example (Nuliana, January):**

  | XForce | Xpeers In Target | Avg Xpeer Index | Shrinkage | XForce Index |
  | --- | --- | --- | --- | --- |
  | Nuliana | 58.3% | 93.8% | 19.1% | 84% |
  | Nuleo | 71.9% | 86.6% | 28.7% | 83.3% |

  `Average XForce Index = (84 + 83.3) / 2 = 83.7%`

- **Edge Cases / Legacy Quirks:** _TBD._

---

## Social Media

### Xpeer (Social Media)

#### 1. Adherence

- **Description:** Same logic and datasets as Core & Fraud → [Adherence](#1-adherence).
- **Edge Cases / Legacy Quirks:** _TBD — confirm there are no Social Media-specific filters in the legacy SM notebook._

#### 2. Normalized Occupancy (NO)

- **Description:** Agent's occupancy divided by the average occupancy for their team and shift. Occupancy is the percentage of dimensioned time the agent was working on jobs **via Sprinklr** (instead of shuffle).
- **Formula:** `Occupancy / Occupancy Benchmark`
- **Target:** `>= 100%`
- **Datasets:**
  - `agent_dimensioned_activities`
  - `usr.sprinklr_api_data_integration.sprinklr_normalized_occupancy_data`
- **Filters & Caveats:**
  - Dimensioned activities **not** taken into account: `lunch_break`, `time_off`, `shrinkage`.
- **Example (Nuberto, January):**

  | Working Time on Jobs | Dimensioned Time | Avg Squad/Shift Occupancy (Benchmark) |
  | --- | --- | --- |
  | 6.3 h | 7.5 h | 92% |

  `Occupancy = 6.3 / 7.5 = 84%` → `NO = 84% / 92% = 91.3%`

- **Edge Cases / Legacy Quirks:** _TBD — Sprinklr case assignment/unassignment time may have gaps; confirm legacy handling._

#### 3. Quality

- **Description:** Same averaging logic as Core & Fraud → [Quality](#4-quality), but SM unions **two** feeds: **Playvox** (`qmo_playvox_consolidated`) and, from **2026-05-01** onward, **Sprinklr SM** (`mx__series_contract.social_media_case_summary_information`, resolved via `usr.mx__enablement.sprinklr_sm_users`).
- **Edge Cases / Legacy Quirks:** The legacy Core/Fraud Quality dataset carried a `UNION ALL` from the Sprinklr SM case-QA table, but those rows were **dead code** — the active-roster join excluded `social`, so they never reached `usr.mx__cx.quality_io`. The new pipeline keeps social agents in the roster, so the Sprinklr SM rows (`>= 2026-05-01`, tagged `source='sprinklr_sm'`) now actually score SM Quality. Both feeds are on the same 0-100 scale and are averaged together (additive `UNION ALL`, not a replacement of Playvox).

#### 4. Human tNPS

- **Description:** NPS evaluation of survey responses for social media. Promoters are responses `>= 9`; detractors are responses `<= 6`.
- **Formula:** `(Promoters - Detractors) / Total Valid NPS Responses`
- **Target:** `>= 88%`
- **Datasets:** `usr.sprinklr_api_data_integration.sprinklr_tnps_data`.
- **Filters & Caveats:**
  - Promoters: responses `>= 9`.
  - Detractors: responses `<= 6`.
- **Example (Nuberto, January):** Scores `[9, 10, 5, 7, 10]` → promoters = 3, detractors = 1, total = 5 → `(3 - 1) / 5 = 40%`.
- **Edge Cases / Legacy Quirks:** _TBD — define "valid NPS responses" precisely; confirm passives (7–8) are excluded from numerator only._

#### 5. WoWs

- **Description:** Number of WoW experiences delivered by Social Media Xpeers to Nu's clients.
- **Formula:** `Total Number of WoWs`
- **Target:** `>= 5`
- **Datasets:**
  - `gsheets.sheets.mx_wows_social_media`
  - WoWs Google Sheet
- **Filters & Caveats:** N/A.
- **Example:** Nuberto delivered 7 WoWs in January.
- **Edge Cases / Legacy Quirks:** _TBD — legacy uses `gsheets.sheets.mx_wows_daniel_temp`; confirm canonical source._

#### 6. Xpeer Index

- **Description:** Weighted index combining the 5 SM Xpeer performance metrics (Adherence, NO, Quality, Human tNPS, WoWs) into a single metric so agents can be compared across teams/squads.
- **Formula:** `(Adherence + NO + Quality + Human tNPS + WoWs_transformed) / 5`
- **Target:** `>= 95%`
- **Datasets:** Union of datasets for Adherence, NO, Quality, Human tNPS, and WoWs.
- **Filters & Caveats:**
  - If `NO > 100%`, truncate to `100%`.
  - If `WoWs >= 5`, truncate to `100%`.
  - If `WoWs < 5`, transform as `WoWs / 5` (i.e. `# of WoWs * 100 / 5`).
- **Example A (Nuberto):**

  | Adherence | NO | Quality | Human tNPS | WoWs | Xpeer Index |
  | --- | --- | --- | --- | --- | --- |
  | 90.7% | 91.3% | 89.7% | 40% | 7 | `(90.7 + 91.3 + 89.7 + 40 + 100) / 5 = 82.3%` |

- **Example B (Nurissa):**

  | Adherence | NO | Quality | Human tNPS | WoWs | Xpeer Index |
  | --- | --- | --- | --- | --- | --- |
  | 99.3% | 113.2% | 95% | 100% | 3 | `(99.3 + 100 + 95 + 100 + (3 * 100 / 5)) / 5 = 94.9%` |

- **Edge Cases / Legacy Quirks:** _TBD._

---

### XForce (Social Media)

Same metrics and logic as Core & Fraud → [XForce (Core & Fraud)](#xforce-core--fraud), **except Improved Benchmarks** (per source doc 2026-06-09). Document any other Social Media-specific differences here as they surface during migration.

- **Edge Cases / Legacy Quirks:** _TBD — Average Xpeer Index uses the SM Xpeer Index variant. SM excludes Improved Benchmarks (also note SM has no NTPJ)._

---

### XPLead (Social Media)

Same metrics and logic as Core & Fraud → [XPLead (Core & Fraud)](#xplead-core--fraud). Document any Social Media-specific differences here as they surface during migration.

- **Edge Cases / Legacy Quirks:** _TBD._

---

## Content

### Xpeer (Content)

#### 1. Adherence

- **Description:** Same logic and datasets as Core & Fraud → [Adherence](#1-adherence).
- **Edge Cases / Legacy Quirks:** _TBD — Content agent roster comes from `gsheets.sheets.mx_content_bdx` (with `valid_from`/`valid_to` ranges) instead of `cx_mx_bdx_snapshots`. Confirm Adherence still uses the same datasets and that the roster swap doesn't change the calculation._

#### 2. Normalized Time Per Job (NTPJ) — SLA-weighted compliance

- **Description:** Despite the name (kept for standardization across teams), Content NTPJ is **not** the duration `actual/expected` ratio Core & Fraud use. It is a **jobs-within-SLA compliance** metric — the share of SLA-weighted work delivered within its SLA. Legacy calls it `ntpj_sla_old` and promotes it to `metric='ntpj_agent'`.
- **Formula:** `SUM(sla_seconds of on-time jobs) / SUM(sla_seconds) * 100`. A job is "on time" iff `actual_seconds <= sla_seconds`; crediting is **all-or-nothing** (an on-time job credits its full `sla_seconds`, a late job credits 0).
- **Target:** `>= 95%` (higher is better; bounded ≤ 100).
- **Job grain:** `macros` / `faq` / `ar` → one job = one source row; every other type → one job = one distinct `content_id` (MOS ticket), summing `net_time_spent_seconds`.
- **SLA map:** per-job-type OLD-SLA seconds from the **`Content - SLAs`** sheet tab (`adj_content_slas`). An INNER JOIN drops job types with no Content SLA (`mastery_cx`, `sop`, generic `projects`, stray Core/Fraud OOS types).
- **Datasets:** `taskmaster_consolidated_registry` (OOS only; `content_id` parsed from `comment` / `ticket__id`), `agent_information` (Content roster), `adj_content_slas` (SLA map).
- **Filters & Caveats:** Content agents only; `date >= 2025-12-01`; outage dates `2026-03-10 / 2026-03-27 / 2026-04-09` dropped (before the `content_id` grouping). Content OOS tracking begins March 2026.
- **Example (aura.olvera, March 2026):** `358,800 / 471,000 = 76.18%` — cluster-verified byte-for-byte vs legacy `ntpj_sla_old`.
- **Consumers:** Xpeer Index (Content) adds this **raw** (NOT folded around 100, unlike Core/Fraud duration NTPJ); `ntpj_xforce` (Content) counts agents `>= 95`.
- **Implementation:** substrate `metrics_data/jobs_within_sla.py` → `io_jobs_within_sla_raw`; metric `metrics/content_sla_ntpj.py`, unioned into `io_ntpj_metric` by `build_ntpj.py`.

#### 3. Normalized Occupancy (NO)

- **Description:** Agent's occupancy divided by the average occupancy for their team and shift. Occupancy is the percentage of dimensioned time the agent was working on jobs (shuffle + out of shuffle).
- **Formula:** `Occupancy / Occupancy Benchmark`
- **Target:** `>= 100%`
- **Datasets:**
  - `agent_dimensioned_activities`
  - `taskmaster_consolidated_registry`
- **Filters & Caveats:**
  - Dimensioned activities **not** taken into account: `lunch_break`, `time_off`, `shrinkage`.
  - Only jobs in `finished`, `transferred`, `skipped`.
- **Example (Nuberto, January):**

  | Working Time on Jobs | Dimensioned Time | Avg Squad/Shift Occupancy (Benchmark) |
  | --- | --- | --- |
  | 6.3 h | 7.5 h | 92% |

  `Occupancy = 6.3 / 7.5 = 84%` → `NO = 84% / 92% = 91.3%`

- **Edge Cases / Legacy Quirks:** _TBD._

#### 4. Quality (CSAT)

- **Description:** Customer satisfaction results from the monthly survey the Content team runs on Xpeers. Each question is answered on a 1–5 scale; only answers `>= 4` count as promoters.
- **Formula:** `Promoters / Total Questions`
- **Target:** `>= 95%`
- **Datasets:** `gsheets.sheets.mx_content_csat`.
- **Filters & Caveats:**
  - Source is the monthly survey run by the Content team.
  - There are **8 questions** in total per survey.
  - An answer counts as a promoter if `>= 4`.
- **Example (Nuberto, January):**

  | Survey Response | # Promoters | # Questions |
  | --- | --- | --- |
  | #1 | 7 | 8 |
  | #2 | 4 | 8 |
  | #3 | 8 | 8 |
  | #4 | 7 | 8 |
  | #5 | 6 | 8 |

  `CSAT = (7 + 4 + 8 + 7 + 6) / (8 * 5) = 80%`

- **Edge Cases / Legacy Quirks:** _TBD — confirm the canonical `mx_content_csat` sheet schema (this dataset is not yet documented in `legacy/CLAUDE.md`)._

#### 5. Xpeer Index

- **Description:** Weighted index combining the 4 Content Xpeer performance metrics (Adherence, NTPJ, NO, Quality/CSAT) into a single metric so agents can be compared across teams/squads.
- **Formula:** `(Adherence + NTPJ + NO + Quality) / 4`
- **Target:** `>= 95%`
- **Datasets:**
  - `agent_productivity`
  - `agent_dimensioned_activities`
  - `staffing_hero__actor_status_status_history`
  - `staffing_hero__actor_statuses`
  - `staffing_hero__status_options`
  - `taskmaster_consolidated_registry`
  - `qmo_playvox_consolidated`
  - `gsheets.sheets.mx_content_csat`
- **Filters & Caveats:**
  - If `NTPJ > 100%`, apply `100% - (NTPJ - 100%)`.
  - If `NO > 100%`, truncate to `100%`.
- **Example A (Nuberto):**

  | Adherence | NTPJ | NO | Quality (CSAT) | Xpeer Index |
  | --- | --- | --- | --- | --- |
  | 90.7% | 84.6% | 91.3% | 80% | `(90.7 + 84.6 + 91.3 + 80) / 4 = 86.7%` |

- **Example B (Nurissa):**

  | Adherence | NTPJ | NO | Quality (CSAT) | Xpeer Index |
  | --- | --- | --- | --- | --- |
  | 99.3% | 107.4% | 113.2% | 95% | `(99.3 + [100 - (107.4 - 100)] + 100 + 95) / 4 = 96.7%` |

- **Edge Cases / Legacy Quirks:** _TBD — confirm whether the `qmo_playvox_consolidated` dependency is correct (PDF lists it for Content Xpeer Index even though Content uses CSAT, not Playvox)._

---

### XForce (Content)

Same metrics and logic as Core & Fraud → [XForce (Core & Fraud)](#xforce-core--fraud). Document any Content-specific differences here as they surface during migration.

- **Edge Cases / Legacy Quirks:** _TBD._

---

### XPLead (Content)

Same metrics and logic as Core & Fraud → [XPLead (Core & Fraud)](#xplead-core--fraud). Document any Content-specific differences here as they surface during migration.

- **Edge Cases / Legacy Quirks:** _TBD._

---

## Cross-cutting Notes

- **Granularity.** All metrics are computed at **daily**, **weekly**, and **monthly** granularities in the legacy pipeline. Weekly/monthly aggregations use `FIRST_VALUE(... ORDER BY date DESC)` to pick the most-recent value for hierarchy fields (XForce, XPLead, squad).
- **Rankings.** Quartile rankings are computed via `NTILE(4)` at both general and team (squad) levels.
- **Squad exclusions.** Squads `social` and `content` are excluded from the Core & Fraud analysis.
- **Manual adjustments.** Today these are hardcoded in legacy SQL. In the new pipeline they move to a Google Sheet and are applied via the Adjustments layer (see `PROJECT_CONTEXT.md`). When migrating each metric, list **every historical adjustment** under the relevant metric's _Edge Cases / Legacy Quirks_ section so we can audit them.

## Open Questions / TODO

Use this section as a living list of items that need answers before (or while) migrating each metric:

- [ ] Confirm "less than 2 full months" definition for Nuvinhos against `cx_mx_bdx_snapshots`.
- [ ] Reconcile the Improved Benchmarks example arithmetic vs the description ("stayed the same = improved").
- [ ] Confirm exact Shrinkage `dime_invalid_notation` values (e.g. `Mouring` vs `Mourning`).
- [x] SM Quality datasets: Playvox + Sprinklr SM (`social_media_case_summary_information`, `>= 2026-05-01`), unioned in `metrics_data/quality_evaluations.py`.
- [ ] Confirm Content Xpeer Index datasets (PDF lists `qmo_playvox_consolidated` despite Content using CSAT).
- [ ] Confirm canonical WoWs source (PDF: `mx_wows_social_media`, legacy: `mx_wows_daniel_temp`).
- [ ] Document the full list of historical hardcoded adjustments per metric.
- [x] ~~Confirm pre/post-March-2026 Xpeer Index behaviour (NO inclusion).~~ Confirmed: NO is included in the Index from **March 2026** onward (source doc 2026-05-27).
- [x] ~~Confirm NTPJ benchmark window switch.~~ Confirmed: Core & Fraud switched 4-month → current-month in **April 2026**; Content still uses the 4-month window (source doc 2026-05-27).
- [x] ~~Confirm the post-May-2026 XForce Index composition.~~ Confirmed: **3 components** (Xpeers In Target, Average Xpeer Index, Shrinkage); Improved Benchmarks removed **April 2026 for Core, May 2026 for all teams** (source doc 2026-06-09). SM XForce also excludes Improved Benchmarks.
- [x] ~~Confirm whether **Fraud** drops Improved Benchmarks on the Core date (April 2026) or the "all teams" date (May 2026).~~ Confirmed: **Fraud drops it from May 2026** (Core April 2026); never applied to SM/Content. Improved Benchmarks is **no longer computed or reported** — the new pipeline suppresses it from each team's cutover onward.
- [ ] Decide whether pre-removal XForce Index history is recomputed without Improved Benchmarks or keeps the original 4-component definition.
- [ ] Resolve the Improved Benchmarks example inconsistencies (numerator counting of "stayed the same"; apparent inverted "improved" label on the Time-Per-Job table).
