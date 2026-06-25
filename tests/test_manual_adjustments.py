"""Tests for approved manual-adjustment helper semantics."""

from __future__ import annotations

import datetime as dt

import pandas as pd

from adjustments.manual import (
    apply_no_shrinkage,
    drop_cross_support_jobs,
    drop_excluded_jobs,
    drop_slot_windows,
    reclassify_dime_slots,
)


def _slot(**overrides) -> dict:
    base = {
        "agent": "ana.nu",
        "team": "core",
        "xplead": "lead.one",
        "squad": "lifecycle",
        "date": dt.date(2026, 5, 4),
        "slot_time": "12:00:00",
        "activity_type_required": "available",
        "shrinkage_flag": 0,
        "controllable_shrinkage_flag": 0,
        "uncontrollable_shrinkage_flag": 0,
    }
    base.update(overrides)
    return base


def _window(**overrides) -> dict:
    base = {
        "Equipo": "Core",
        "Agente": "ana.nu",
        "Fecha Inicio": "2026-05-04",
        "Fecha Fin": "2026-05-04",
        "Hora Inicio": "11:30",
        "Hora Fin": "13:00",
    }
    base.update(overrides)
    return base


def test_reclassify_dime_slots_updates_activity_and_shrinkage_flags():
    slots = pd.DataFrame([_slot()])
    adjustments = pd.DataFrame([
        {
            **_window(),
            "Etiqueta Correcta": "shrinkage",
        }
    ])

    out = reclassify_dime_slots(slots, adjustments)

    assert out.iloc[0]["activity_type_required"] == "shrinkage"
    assert out.iloc[0]["shrinkage_flag"] == 1


def test_drop_slot_windows_supports_todos_xplead_scope():
    slots = pd.DataFrame([
        _slot(agent="ana.nu", xplead="david.fernandez"),
        _slot(agent="bea.nu", xplead="other.lead"),
    ])
    adjustments = pd.DataFrame([
        {
            **_window(),
            "Agente": "Todos (XPLead: david.fernandez)",
            "Hora Inicio": "0:00",
            "Hora Fin": "23:59",
        }
    ])

    out = drop_slot_windows(slots, adjustments)

    assert out["agent"].tolist() == ["bea.nu"]


def test_apply_no_shrinkage_keeps_required_slot_but_clears_numerator_flags():
    slots = pd.DataFrame([
        _slot(activity_type_required="shrinkage", shrinkage_flag=1, controllable_shrinkage_flag=1)
    ])

    out = apply_no_shrinkage(slots, pd.DataFrame([_window()]))

    assert len(out) == 1
    assert out.iloc[0]["activity_type_required"] == "shrinkage"
    assert out.iloc[0]["shrinkage_flag"] == 0
    assert out.iloc[0]["controllable_shrinkage_flag"] == 0


def test_job_exclusions_match_queues_and_squad_scope():
    jobs = pd.DataFrame([
        {
            "agent": "ana.nu",
            "team": "core",
            "squad": "lifecycle",
            "date": dt.date(2026, 5, 26),
            "job_type": "backoffice-multiproduct-credit-account-cancellation",
            "job_id": "bko - backoffice-multiproduct-credit-account-cancellation - finished",
        },
        {
            "agent": "ana.nu",
            "team": "core",
            "squad": "cuenta",
            "date": dt.date(2026, 5, 26),
            "job_type": "backoffice-payment-srf",
            "job_id": "bko - backoffice-payment-srf - finished",
        },
    ])
    cross_support = pd.DataFrame([
        {
            "Equipo": "Core",
            "Agente": "ana.nu",
            "Queues a Excluir": "backoffice-payment-srf",
            "Fecha Inicio": "2026-05-01",
            "Fecha Fin": "9000-01-01",
        }
    ])
    job_exclusions = pd.DataFrame([
        {
            "Equipo": "Core",
            "Agente": "Todos (squad lifecycle)",
            "Job (Clasificación)": "backoffice-multiproduct-credit-account-cancellation",
            "Fecha Inicio": "2026-05-01",
            "Fecha Fin": "9000-01-01",
        }
    ])

    out = drop_excluded_jobs(drop_cross_support_jobs(jobs, cross_support), job_exclusions)

    assert out.empty
