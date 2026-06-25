import pandas as pd

from scripts.adjustments_scripts.download_adjustments import TabReport, approved_only, check_tab


def test_approved_only_filters_tabs_with_status_column() -> None:
    df = pd.DataFrame(
        {
            "Agente": ["approved.agent", "pending.agent", "denied.agent"],
            "Estatus": [" Aprobado ", "Pendiente", "Denegado"],
            "Equipo": ["Core", "Fraud", "Social Media"],
        }
    )

    result = approved_only(df)

    assert result["Agente"].tolist() == ["approved.agent"]


def test_approved_only_leaves_tabs_without_status_column_unchanged() -> None:
    df = pd.DataFrame({"Alias": ["a"], "Agente": ["approved.agent"]})

    result = approved_only(df)

    pd.testing.assert_frame_equal(result.reset_index(drop=True), df)


def test_check_tab_rejects_legacy_all_day_and_indefinido_values() -> None:
    df = pd.DataFrame(
        {
            "Equipo": ["Core"],
            "Agente": ["approved.agent"],
            "Fecha Inicio": ["2026-01-01"],
            "Fecha Fin": ["Indefinido"],
            "Hora Inicio": ["All day"],
            "Hora Fin": ["All day"],
            "Estatus": ["Aprobado"],
        }
    )
    report = TabReport("Example")

    check_tab(df, report)

    assert any("Fecha Fin" in error for error in report.errors)
    assert sum("HH:MM time" in error for error in report.errors) == 2
