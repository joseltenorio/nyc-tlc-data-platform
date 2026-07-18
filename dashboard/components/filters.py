from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd
import streamlit as st

from dashboard.config import load_dashboard_config


SERVICE_LABELS = {
    "yellow": "Yellow",
    "green": "Green",
    "fhv": "FHV",
    "fhvhv": "HVFHV",
}


@dataclass(frozen=True)
class ScopeSelection:
    years: list[int]
    months: list[int]
    services: list[str]


def _available(frame: pd.DataFrame, column: str) -> list:
    if frame.empty or column not in frame.columns:
        return []
    return sorted(value for value in frame[column].dropna().unique().tolist())


def render_scope_filters(
    frame: pd.DataFrame,
    *,
    key: str,
    year_col: str = "source_year",
    month_col: str = "source_month",
    service_col: str = "service_type",
    show_month: bool = True,
) -> ScopeSelection:
    years = [int(value) for value in _available(frame, year_col)]
    months = [int(value) for value in _available(frame, month_col)] if show_month else []
    services = [str(value) for value in _available(frame, service_col)]

    st.sidebar.markdown("### Filtros")
    selected_years = st.sidebar.multiselect(
        "Año",
        years,
        default=years,
        key=f"{key}_years",
    )
    selected_months = months
    if show_month and months:
        selected_months = st.sidebar.multiselect(
            "Mes",
            months,
            default=months,
            format_func=lambda value: f"{int(value):02d}",
            key=f"{key}_months",
        )
    selected_services = st.sidebar.multiselect(
        "Servicio",
        services,
        default=services,
        format_func=lambda value: SERVICE_LABELS.get(str(value).lower(), str(value).upper()),
        key=f"{key}_services",
    )
    return ScopeSelection(
        years=[int(value) for value in selected_years],
        months=[int(value) for value in selected_months],
        services=[str(value) for value in selected_services],
    )


def apply_scope(
    frame: pd.DataFrame,
    selection: ScopeSelection,
    *,
    year_col: str = "source_year",
    month_col: str = "source_month",
    service_col: str = "service_type",
) -> pd.DataFrame:
    result = frame.copy()
    if result.empty:
        return result
    if selection.years and year_col in result.columns:
        result = result[result[year_col].isin(selection.years)]
    if selection.months and month_col in result.columns:
        result = result[result[month_col].isin(selection.months)]
    if selection.services and service_col in result.columns:
        result = result[result[service_col].astype(str).isin(selection.services)]
    return result


def coverage_note(selection: ScopeSelection) -> str | None:
    scope = load_dashboard_config().get("analysis_scope", {})
    partial = scope.get("partial_services", {})
    messages: list[str] = []
    for service, details in partial.items():
        if service not in selection.services:
            continue
        available_years = {int(value) for value in details.get("available_years", [])}
        missing = sorted(set(selection.years) - available_years)
        if missing:
            years = ", ".join(str(value) for value in missing)
            messages.append(
                f"{SERVICE_LABELS.get(service, service.upper())}: sin cobertura Gold para {years}; se excluye del cálculo, no se imputa como cero."
            )
    return " ".join(messages) if messages else None


def select_single(
    label: str,
    values: Iterable,
    *,
    key: str,
    format_func=None,
):
    options = list(values)
    if not options:
        return None
    return st.sidebar.selectbox(label, options, key=key, format_func=format_func)
