from __future__ import annotations

import math

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.components.layout import (
    empty_state,
    format_compact,
    format_duration,
    kpi_card,
    note,
    render_page_header,
    section_title,
)
from dashboard.components.page_common import bootstrap_page
from dashboard.components.styles import STATUS_COLORS, style_plotly
from dashboard.data_access.audit_repository import AuditRepository




def numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(0, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce").fillna(0)

bootstrap_page()
render_page_header(
    "Auditoría y calidad del pipeline",
    "Corridas, Parquet procesados, cobertura esperada, reglas de calidad, errores y reintentos HTTP.",
    "Control operativo",
)
audit = AuditRepository().load()
runs = audit.runs.copy()
datasets = audit.datasets.copy()
coverage = audit.coverage.copy()
quality = audit.quality.copy()
attempts = audit.attempts.copy()
errors = audit.errors.copy()
reconciliations = audit.reconciliations.copy()

if runs.empty:
    empty_state(
        "No hay corridas registradas",
        "Ejecuta Bronze, Silver, Gold o ML. El dashboard no genera datos sintéticos.",
    )
    st.stop()

for column in ("started_at", "finished_at"):
    if column in runs.columns:
        runs[column] = pd.to_datetime(runs[column], utc=True, errors="coerce")
runs["status"] = runs.get("status", "UNKNOWN").fillna("UNKNOWN").astype(str).str.upper()
runs["layer"] = runs.get("layer", "unknown").fillna("unknown").astype(str).str.lower()

layers = sorted(runs["layer"].dropna().unique().tolist())
selected_layers = st.sidebar.multiselect(
    "Capas de auditoría", layers, default=layers, key="audit_layers"
)
filtered_runs = runs[runs["layer"].isin(selected_layers)].copy()

if not datasets.empty and "layer" in datasets.columns:
    datasets = datasets[datasets["layer"].astype(str).str.lower().isin(selected_layers)]
if not coverage.empty and "layer" in coverage.columns:
    coverage = coverage[coverage["layer"].astype(str).str.lower().isin(selected_layers)]
if not quality.empty and "layer" in quality.columns:
    quality = quality[quality["layer"].astype(str).str.lower().isin(selected_layers)]
if not attempts.empty and "layer" in attempts.columns:
    attempts = attempts[attempts["layer"].astype(str).str.lower().isin(selected_layers)]

finished = filtered_runs[filtered_runs["status"] != "RUNNING"]
success_rate = (
    finished["status"].isin(["SUCCESS", "SUCCEEDED", "PARTIAL_SUCCESS"]).mean()
    if not finished.empty
    else None
)
parquet_processed = (
    int(numeric_series(datasets, "parquet_files").sum())
    if not datasets.empty
    else 0
)
quality_failed = int(
    quality.get("status", pd.Series(dtype=str)).astype(str).str.upper().eq("FAILED").sum()
) if not quality.empty else 0
missing_expected = (
    int(numeric_series(coverage, "missing_count").sum())
    if not coverage.empty
    else 0
)
if not attempts.empty and "attempt_kind" in attempts.columns:
    download_attempts = attempts[attempts["attempt_kind"] == "download"].copy()
else:
    download_attempts = pd.DataFrame()
retries = (
    int(numeric_series(download_attempts, "retry_number").gt(0).sum())
    if not download_attempts.empty
    else 0
)
median_duration = numeric_series(filtered_runs, "duration_seconds").replace(0, pd.NA).median()

cols = st.columns(6)
with cols[0]:
    kpi_card("Corridas", format_compact(len(filtered_runs)), "Todas las capas seleccionadas")
with cols[1]:
    kpi_card(
        "Tasa operativa",
        "—" if success_rate is None or math.isnan(success_rate) else f"{success_rate:.1%}",
        "SUCCESS + PARTIAL_SUCCESS",
    )
with cols[2]:
    kpi_card("Parquet registrados", format_compact(parquet_processed), "Entradas y salidas auditadas")
with cols[3]:
    kpi_card("Reintentos HTTP", format_compact(retries), "Intentos posteriores al primero", color="#F2B35D")
with cols[4]:
    kpi_card("Reglas fallidas", format_compact(quality_failed), "Calidad y reconciliación", color="#FF826B")
with cols[5]:
    kpi_card("Esperados ausentes", format_compact(missing_expected), format_duration(median_duration), color="#F2B35D")

note(f"Fuente activa: {audit.source_note}.")

tab_summary, tab_datasets, tab_coverage, tab_quality, tab_attempts, tab_runs = st.tabs(
    [
        "Resumen",
        "Parquet por capa",
        "Cobertura esperada",
        "Calidad",
        "Errores y reintentos",
        "Corridas",
    ]
)

with tab_summary:
    left, right = st.columns(2)
    with left:
        section_title("Corridas por capa y estado")
        summary = filtered_runs.groupby(["layer", "status"], as_index=False).size()
        fig = px.bar(
            summary,
            x="layer",
            y="size",
            color="status",
            barmode="stack",
            labels={"layer": "Capa", "size": "Corridas", "status": "Estado"},
            color_discrete_map=STATUS_COLORS,
        )
        st.plotly_chart(style_plotly(fig, height=390), width="stretch")
    with right:
        section_title("Duración por capa")
        duration = filtered_runs.copy()
        duration["duration_seconds"] = pd.to_numeric(
            duration.get("duration_seconds"), errors="coerce"
        )
        duration = duration.dropna(subset=["duration_seconds"])
        if duration.empty:
            empty_state("Sin duraciones", "Las corridas finalizarán este cálculo al terminar.")
        else:
            fig = px.box(
                duration,
                x="layer",
                y="duration_seconds",
                points="all",
                labels={"layer": "Capa", "duration_seconds": "Segundos"},
            )
            st.plotly_chart(style_plotly(fig, height=390), width="stretch")

with tab_datasets:
    section_title("Archivos Parquet procesados y publicados")
    if datasets.empty:
        empty_state("Sin eventos de datasets", "Las nuevas ejecuciones registrarán cada entrada y salida física.")
    else:
        data = datasets.copy()
        data["parquet_files"] = numeric_series(data, "parquet_files")
        data["bytes_on_disk"] = numeric_series(data, "bytes_on_disk")
        grouped = data.groupby(["layer", "dataset_type", "status"], as_index=False).agg(
            parquet_files=("parquet_files", "sum"),
            bytes_on_disk=("bytes_on_disk", "sum"),
            events=("dataset_name", "count"),
        )
        fig = px.bar(
            grouped,
            x="layer",
            y="parquet_files",
            color="dataset_type",
            barmode="stack",
            labels={"layer": "Capa", "parquet_files": "Parquet", "dataset_type": "Tipo"},
        )
        st.plotly_chart(style_plotly(fig, height=420), width="stretch")
        shown = [
            column
            for column in (
                "recorded_at",
                "layer",
                "dataset_name",
                "dataset_type",
                "operation",
                "status",
                "service",
                "year",
                "month",
                "parquet_files",
                "rows",
                "bytes_on_disk",
                "path",
                "error_type",
                "error_message",
            )
            if column in data.columns
        ]
        st.dataframe(data[shown], width="stretch", hide_index=True)

with tab_coverage:
    section_title("Datasets y periodos esperados")
    if coverage.empty:
        empty_state("Sin fotografías de cobertura", "Ejecuta nuevamente las capas con la auditoría unificada.")
    else:
        view = coverage.copy()
        view["coverage_rate"] = pd.to_numeric(view.get("coverage_rate"), errors="coerce")
        fig = px.bar(
            view,
            x="layer",
            y="coverage_rate",
            color="status",
            range_y=[0, 1],
            labels={"layer": "Capa", "coverage_rate": "Cobertura", "status": "Estado"},
            color_discrete_map={"COMPLETE": "#8EDB8A", "PARTIAL": "#F2B35D"},
        )
        st.plotly_chart(style_plotly(fig, height=390), width="stretch")
        shown = [
            column
            for column in (
                "checked_at",
                "execution_id",
                "layer",
                "status",
                "expected_count",
                "available_count",
                "ready_count",
                "missing_count",
                "not_applicable_count",
                "not_published_count",
                "deferred_count",
                "coverage_rate",
                "missing",
            )
            if column in view.columns
        ]
        st.dataframe(view[shown], width="stretch", hide_index=True)

with tab_quality:
    section_title("Reglas de calidad y reconciliación")
    if quality.empty and reconciliations.empty:
        empty_state("Sin resultados de calidad", "Bronze, Silver, Gold y ML los registrarán al ejecutarse.")
    else:
        if not quality.empty:
            q = quality.copy()
            q["status"] = q.get("status", "UNKNOWN").astype(str).str.upper()
            quality_summary = q.groupby(["layer", "dimension", "status"], as_index=False).size()
            fig = px.bar(
                quality_summary,
                x="layer",
                y="size",
                color="status",
                barmode="stack",
                labels={"layer": "Capa", "size": "Reglas", "status": "Estado"},
                color_discrete_map=STATUS_COLORS,
            )
            st.plotly_chart(style_plotly(fig, height=390), width="stretch")
            shown = [
                column
                for column in (
                    "checked_at",
                    "layer",
                    "dataset_name",
                    "rule_code",
                    "dimension",
                    "severity",
                    "status",
                    "expected",
                    "actual",
                    "failed_rows",
                    "message",
                )
                if column in q.columns
            ]
            st.dataframe(q[shown], width="stretch", hide_index=True)
        if not reconciliations.empty:
            st.markdown("#### Reconciliaciones de filas")
            st.dataframe(reconciliations, width="stretch", hide_index=True)

with tab_attempts:
    section_title("Descargas, intentos y errores")
    if not download_attempts.empty:
        d = download_attempts.copy()
        d["attempt_number"] = pd.to_numeric(d.get("attempt_number"), errors="coerce")
        attempt_summary = d.groupby(["service", "outcome"], as_index=False).size()
        fig = px.bar(
            attempt_summary,
            x="service",
            y="size",
            color="outcome",
            barmode="stack",
            labels={"service": "Servicio", "size": "Intentos", "outcome": "Resultado"},
        )
        st.plotly_chart(style_plotly(fig, height=360), width="stretch")
        st.dataframe(d, width="stretch", hide_index=True)
    else:
        empty_state("Sin intentos HTTP detallados", "Se registrarán hasta cinco reintentos por descarga Bronze.")
    if not errors.empty:
        st.markdown("#### Errores consolidados")
        st.dataframe(errors, width="stretch", hide_index=True)

with tab_runs:
    section_title("Historial de corridas")
    shown = [
        column
        for column in (
            "started_at",
            "finished_at",
            "execution_id",
            "layer",
            "execution_type",
            "status",
            "duration_seconds",
            "metrics.parquet_files_processed",
            "metrics.parquet_partitions_processed",
            "metrics.datasets_built",
            "metrics.models_trained",
            "error_type",
            "error_message",
        )
        if column in filtered_runs.columns
    ]
    st.dataframe(filtered_runs[shown], width="stretch", hide_index=True)
