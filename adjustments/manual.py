"""Pandas helpers for the approved manual-adjustment CSVs.

These helpers intentionally implement the current adjustment tabs directly.
They are small and typed so metric modules can stay pure and tests can pass
synthetic adjustment frames without reading files.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "adjustments" / "data"
MISSING_DIME_PATH = REPO_ROOT / "adjustments" / "slots_faltantes_dime.csv"


def read_adjustment_csv(name: str) -> pd.DataFrame:
    path = DATA_DIR / f"{name}.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


def _empty(df: pd.DataFrame | None) -> bool:
    return df is None or df.empty


def _to_date(value: object) -> pd.Timestamp:
    return pd.Timestamp(str(value).strip()).normalize()


def _time_to_minutes(value: object) -> int:
    h, m = str(value).strip()[:5].split(":", maxsplit=1)
    return int(h) * 60 + int(m)


def _row_window_mask(df: pd.DataFrame, row: pd.Series) -> pd.Series:
    dates = pd.to_datetime(df["date"]).dt.normalize()
    start = _to_date(row["Fecha Inicio"])
    end = _to_date(row["Fecha Fin"])
    date_ok = dates.between(start, end)

    if "slot_time" not in df.columns:
        return date_ok
    slot_minutes = df["slot_time"].astype(str).str.slice(0, 5).map(_time_to_minutes)
    start_min = _time_to_minutes(row["Hora Inicio"])
    end_min = _time_to_minutes(row["Hora Fin"])
    # 23:59 is the full-day sentinel. Treat it as the end of day so the 23:30
    # slot is included while normal windows remain half-open.
    end_exclusive = 24 * 60 if end_min == 23 * 60 + 59 else end_min
    return date_ok & (slot_minutes >= start_min) & (slot_minutes < end_exclusive)


def _scope_mask(df: pd.DataFrame, row: pd.Series) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    team = str(row.get("Equipo", "")).strip().lower()
    if team and team != "todos" and "team" in df.columns:
        mask &= df["team"].astype(str).str.lower() == team

    agent = str(row.get("Agente", "")).strip()
    agent_l = agent.lower()
    if not agent_l or agent_l == "todos":
        return mask
    if agent_l.startswith("todos (xplead:") and "xplead" in df.columns:
        xplead = agent.split(":", maxsplit=1)[1].rstrip(")").strip().lower()
        return mask & (df["xplead"].astype(str).str.lower() == xplead)
    if agent_l.startswith("todos (squad") and "squad" in df.columns:
        squad = agent.split("squad", maxsplit=1)[1].rstrip(")").strip().lower()
        return mask & df["squad"].astype(str).str.lower().str.contains(
            squad, regex=False, na=False
        )
    if "agent" in df.columns:
        return mask & (df["agent"].astype(str).str.lower() == agent_l)
    return mask


def _combined_window_mask(df: pd.DataFrame, adjustments: pd.DataFrame) -> pd.Series:
    if _empty(adjustments) or df.empty:
        return pd.Series(False, index=df.index)
    mask = pd.Series(False, index=df.index)
    for _, row in adjustments.iterrows():
        mask |= _scope_mask(df, row) & _row_window_mask(df, row)
    return mask


def reclassify_dime_slots(slots: pd.DataFrame, inconsistencies: pd.DataFrame | None) -> pd.DataFrame:
    if _empty(inconsistencies) or slots.empty:
        return slots.copy()
    out = slots.copy()
    for _, row in inconsistencies.iterrows():
        label = str(row.get("Etiqueta Correcta", "")).strip()
        if not label:
            continue
        mask = _scope_mask(out, row) & _row_window_mask(out, row)
        out.loc[mask, "activity_type_required"] = label
        if "shrinkage_flag" in out.columns:
            out.loc[mask, "shrinkage_flag"] = int(label == "shrinkage")
            if label != "shrinkage":
                for col in ("controllable_shrinkage_flag", "uncontrollable_shrinkage_flag"):
                    if col in out.columns:
                        out.loc[mask, col] = 0
    return out


def drop_slot_windows(df: pd.DataFrame, *adjustments: pd.DataFrame | None) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    mask = pd.Series(False, index=df.index)
    for adj in adjustments:
        if not _empty(adj):
            mask |= _combined_window_mask(df, adj)
    return df.loc[~mask].copy()


def apply_no_shrinkage(df: pd.DataFrame, no_shrinkage: pd.DataFrame | None) -> pd.DataFrame:
    if _empty(no_shrinkage) or df.empty:
        return df.copy()
    out = df.copy()
    mask = _combined_window_mask(out, no_shrinkage)
    out.loc[mask, "shrinkage_flag"] = 0
    for col in ("controllable_shrinkage_flag", "uncontrollable_shrinkage_flag"):
        if col in out.columns:
            out.loc[mask, col] = 0
    return out


def _job_date_mask(df: pd.DataFrame, row: pd.Series) -> pd.Series:
    dates = pd.to_datetime(df["date"]).dt.normalize()
    return dates.between(_to_date(row["Fecha Inicio"]), _to_date(row["Fecha Fin"]))


def _job_scope_mask(df: pd.DataFrame, row: pd.Series) -> pd.Series:
    return _scope_mask(df, row)


def _contains_any(series: pd.Series, values: list[str]) -> pd.Series:
    haystack = series.astype(str).str.lower()
    mask = pd.Series(False, index=series.index)
    for value in values:
        value = value.strip().lower()
        if value:
            mask |= haystack.str.contains(value, regex=False, na=False)
    return mask


def drop_cross_support_jobs(jobs: pd.DataFrame, cross_support: pd.DataFrame | None) -> pd.DataFrame:
    if _empty(cross_support) or jobs.empty:
        return jobs.copy()
    drop = pd.Series(False, index=jobs.index)
    text = jobs["job_id"].astype(str) + " " + jobs["job_type"].astype(str)
    for _, row in cross_support.iterrows():
        queues = str(row.get("Queues a Excluir", "")).splitlines()
        drop |= _job_scope_mask(jobs, row) & _job_date_mask(jobs, row) & _contains_any(text, queues)
    return jobs.loc[~drop].copy()


def drop_excluded_jobs(jobs: pd.DataFrame, exclusions: pd.DataFrame | None) -> pd.DataFrame:
    if _empty(exclusions) or jobs.empty:
        return jobs.copy()
    drop = pd.Series(False, index=jobs.index)
    text = jobs["job_id"].astype(str) + " " + jobs["job_type"].astype(str)
    for _, row in exclusions.iterrows():
        job = str(row.get("Job (Clasificación)", "")).strip()
        drop |= _job_scope_mask(jobs, row) & _job_date_mask(jobs, row) & _contains_any(text, [job])
    return jobs.loc[~drop].copy()


def append_missing_dime_slots(dime: pd.DataFrame, path: Path = MISSING_DIME_PATH) -> pd.DataFrame:
    if not path.exists():
        return dime.copy()
    missing = pd.read_csv(path, dtype=str).fillna("")
    if missing.empty:
        return dime.copy()
    out = missing.rename(
        columns={
            "agent_dime_squad": "squad",
            "dime_date": "date",
        }
    ).copy()
    out["agent"] = out["agent"].astype(str).str.replace("@nu.com.mx", "", regex=False).str.lower()
    out["date"] = pd.to_datetime(out["date"]).dt.date
    local_ts = pd.to_datetime(out["local_timestamp_dime_slot_starts_at"])
    out["slot_start_local_unix"] = (local_ts.astype("int64") // 1_000_000_000).astype("int64")
    out["slot_end_local_unix"] = out["slot_start_local_unix"] + 30 * 60
    for col in dime.columns:
        if col not in out.columns:
            out[col] = pd.NA
    return pd.concat([dime, out[list(dime.columns)]], ignore_index=True)
