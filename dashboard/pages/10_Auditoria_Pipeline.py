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
        return pd.Series(pd.NA, index=frame.index, dtype="Float64")
    return pd.to_numeric(frame[column], errors="coerce")


def percentage(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{value:.1%}"


bootstrap_page()
render_page_header(
    "Auditoría y calidad del pipeline",
    "Métricas reales desde JSONL, MongoDB, manifests e inventario físico de Parquet.",
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
inventory = audit.inventory.copy()

if all(frame.empty for frame in (runs, datasets, coverage, quality, attempts, inventory)):
    empty_state(
        "No hay auditoría registrada",
        "Ejecuta una capa del pipeline. Esta página no crea datos sintéticos ni completa valores ausentes.",
    )
    st.stop()

if not runs.empty:
    for column in ("started_at", "finished_at"):
        if column in runs.columns:
            runs[column] = pd.to_datetime(runs[column], utc=True, errors="coerce")
    if "status" not in runs.columns:
        runs["status"] = "UNKNOWN"
    runs["status"] = runs["status"].fillna("UNKNOWN").astype(str).str.upper()
    if "layer" not in runs.columns:
        runs["layer"] = "unknown"
    runs["layer"] = runs["layer"].fillna("unknown").astype(str).str.lower()

layer_values: set[str] = set()
for frame in (runs, datasets, coverage, quality, attempts, inventory):
    if not frame.empty and "layer" in frame.columns:
        layer_values.update(frame["layer"].dropna().astype(str).str.lower().tolist())
layers = sorted(layer_values)
selected_layers = st.sidebar.multiselect(
    "Capas de auditoría", layers, default=layers, key="audit_layers"
)


def filter_layers(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "layer" not in frame.columns:
        return frame
    normalized = frame.copy()
    normalized["layer"] = normalized["layer"].astype(str).str.lower()
    return normalized[normalized["layer"].isin(selected_layers)].copy()


filtered_runs = filter_layers(runs)
datasets = filter_layers(datasets)
coverage = filter_layers(coverage)
quality = filter_layers(quality)
attempts = filter_layers(attempts)
errors = filter_layers(errors)
inventory = filter_layers(inventory)

finished = (
    filtered_runs[filtered_runs["status"] != "RUNNING"]
    if not filtered_runs.empty and "status" in filtered_runs.columns
    else pd.DataFrame()
)
success_rate = (
    finished["status"].isin(["SUCCESS", "SUCCEEDED", "PARTIAL_SUCCESS"]).mean()
    if not finished.empty
    else None
)
inventory_parquet = (
    pd.to_numeric(inventory["parquet_files"], errors="coerce")
    if not inventory.empty and "parquet_files" in inventory.columns
    else pd.Series(dtype="float64")
)
inventory_bytes = (
    pd.to_numeric(inventory["bytes_on_disk"], errors="coerce")
    if not inventory.empty and "bytes_on_disk" in inventory.columns
    else pd.Series(dtype="float64")
)
current_parquet = (
    int(inventory_parquet.dropna().sum()) if inventory_parquet.notna().any() else None
)
current_bytes = (
    int(inventory_bytes.dropna().sum()) if inventory_bytes.notna().any() else None
)
failed_dataset_events = (
    int(datasets["status"].astype(str).str.upper().eq("FAILED").sum())
    if not datasets.empty and "status" in datasets.columns
    else None
)
quality_failed = (
    int(quality["status"].astype(str).str.upper().eq("FAILED").sum())
    if not quality.empty and "status" in quality.columns
    else None
)
latest_coverage = coverage.copy()
if not latest_coverage.empty and "layer" in latest_coverage.columns:
    if "checked_at" in latest_coverage.columns:
        latest_coverage["checked_at"] = pd.to_datetime(
            latest_coverage["checked_at"], utc=True, errors="coerce"
        )
        latest_coverage = latest_coverage.sort_values(
            "checked_at", ascending=False, na_position="last"
        )
    latest_coverage = latest_coverage.drop_duplicates("layer", keep="first")
missing_expected = (
    int(pd.to_numeric(latest_coverage["missing_count"], errors="coerce").sum())
    if not latest_coverage.empty and "missing_count" in latest_coverage.columns
    else None
)

download_attempts = (
    attempts[attempts["attempt_kind"].astype(str).eq("download")].copy()
    if not attempts.empty and "attempt_kind" in attempts.columns
    else pd.DataFrame()
)
if not download_attempts.empty:
    attempt_numbers = pd.to_numeric(
        download_attempts.get("attempt_number"), errors="coerce"
    )
    retry_numbers = pd.to_numeric(
        download_attempts.get("retry_number"), errors="coerce"
    )
    if retry_numbers is None and attempt_numbers is not None:
        retry_numbers = (attempt_numbers - 1).clip(lower=0)
    durations = pd.to_numeric(
        download_attempts.get("duration_seconds"), errors="coerce"
    )
    downloaded = pd.to_numeric(
        download_attempts.get("bytes_downloaded"), errors="coerce"
    )
    download_attempts["attempt_number"] = (
        attempt_numbers if attempt_numbers is not None else pd.NA
    )
    download_attempts["retry_number"] = (
        retry_numbers if retry_numbers is not None else pd.NA
    )
    download_attempts["duration_seconds"] = durations if durations is not None else pd.NA
    download_attempts["bytes_downloaded"] = downloaded if downloaded is not None else pd.NA
    final_candidates = download_attempts[
        download_attempts.get("outcome", pd.Series(index=download_attempts.index, dtype=str))
        .astype(str)
        .str.upper()
        .isin(["SUCCESS", "EXHAUSTED"])
    ].copy()
    group_keys = [
        column
        for column in ("execution_id", "service", "year", "month")
        if column in final_candidates.columns
    ]
    if group_keys and not final_candidates.empty:
        final_downloads = (
            final_candidates.sort_values("attempt_number")
            .groupby(group_keys, dropna=False, as_index=False)
            .tail(1)
        )
    else:
        final_downloads = final_candidates
    download_error_rate = (
        ~final_downloads["outcome"].astype(str).str.upper().eq("SUCCESS")
    ).mean() if not final_downloads.empty else None
    retries = (
        int(download_attempts["retry_number"].dropna().gt(0).sum())
        if download_attempts["retry_number"].notna().any()
        else None
    )
    download_seconds = (
        float(download_attempts["duration_seconds"].dropna().sum())
        if download_attempts["duration_seconds"].notna().any()
        else None
    )
    downloaded_bytes = (
        float(download_attempts["bytes_downloaded"].dropna().sum())
        if download_attempts["bytes_downloaded"].notna().any()
        else None
    )
    throughput_mbps = (
        downloaded_bytes * 8 / download_seconds / 1_000_000
        if downloaded_bytes is not None and download_seconds and download_seconds > 0
        else None
    )
else:
    final_downloads = pd.DataFrame()
    download_error_rate = None
    retries = None
    download_seconds = None
    throughput_mbps = None

median_duration = (
    numeric_series(filtered_runs, "duration_seconds").replace(0, pd.NA).median()
    if not filtered_runs.empty
    else None
)

first_row = st.columns(4)
with first_row[0]:
    kpi_card("Corridas", format_compact(len(filtered_runs)), "Capas seleccionadas")
with first_row[1]:
    kpi_card("Tasa operativa", percentage(success_rate), "Éxito total o parcial")
with first_row[2]:
    physical_meta = (
        f"{current_bytes / 1024**3:.2f} GiB en disco"
        if current_bytes is not None
        else "Sin snapshot físico"
    )
    kpi_card("Parquet físicos", format_compact(current_parquet), physical_meta)
with first_row[3]:
    kpi_card("Error de descarga", percentage(download_error_rate), "Resultado final por archivo", color="#FF826B")

second_row = st.columns(4)
with second_row[0]:
    kpi_card("Tiempo de descarga", format_duration(download_seconds), "Suma de intentos reales")
with second_row[1]:
    kpi_card(
        "Velocidad efectiva",
        "—" if throughput_mbps is None else f"{throughput_mbps:.2f} Mbps",
        "Sin métricas de reintento" if retries is None else f"{retries} reintentos HTTP",
    )
with second_row[2]:
    kpi_card("Fallos de datasets", format_compact(failed_dataset_events), "Eventos físicos fallidos", color="#FF826B")
with second_row[3]:
    kpi_card(
        "Reglas fallidas",
        format_compact(quality_failed),
        (
            "Sin snapshot de cobertura"
            if missing_expected is None
            else f"{missing_expected} esperados ausentes · mediana {format_duration(median_duration)}"
        ),
        color="#F2B35D",
    )

note(f"Fuente activa: {audit.source_note}.")

tab_summary, tab_datasets, tab_coverage, tab_quality, tab_attempts, tab_runs = st.tabs(
    [
        "Resumen",
        "Parquet por capa",
        "Cobertura esperada",
        "Calidad",
        "Descargas y errores",
        "Corridas",
    ]
)

with tab_summary:
    left, right = st.columns(2)
    with left:
        section_title("Corridas por capa y estado")
        if filtered_runs.empty:
            empty_state("Sin corridas", "No hay eventos de ejecución para las capas elegidas.")
        else:
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
        section_title("Duración real por capa")
        duration = filtered_runs.copy()
        if not duration.empty:
            duration["duration_seconds"] = numeric_series(duration, "duration_seconds")
            duration = duration[duration["duration_seconds"] > 0]
        if duration.empty:
            empty_state("Sin duraciones", "La duración aparece al finalizar cada corrida.")
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
    section_title("Inventario físico actual de Parquet")
    if inventory.empty:
        empty_state(
            "Sin inventario físico",
            "Se generará data/audit/inventory/medallion_inventory.json al finalizar una corrida.",
        )
    else:
        physical = inventory.copy()
        physical["parquet_files"] = numeric_series(physical, "parquet_files")
        physical["bytes_on_disk"] = numeric_series(physical, "bytes_on_disk")
        fig = px.bar(
            physical,
            x="layer",
            y="parquet_files",
            text="parquet_files",
            labels={"layer": "Capa", "parquet_files": "Archivos Parquet"},
        )
        st.plotly_chart(style_plotly(fig, height=370), width="stretch")
        inventory_columns = [
            column
            for column in (
                "captured_at",
                "layer",
                "root",
                "root_exists",
                "dataset_count",
                "parquet_files",
                "bytes_on_disk",
                "latest_modified_at",
                "scan_error_count",
            )
            if column in physical.columns
        ]
        st.dataframe(physical[inventory_columns], width="stretch", hide_index=True)

        dataset_rows: list[dict[str, object]] = []
        for row in physical.to_dict(orient="records"):
            details = row.get("datasets")
            if not isinstance(details, list):
                continue
            for dataset in details:
                if not isinstance(dataset, dict):
                    continue
                dataset_rows.append(
                    {
                        "layer": row.get("layer"),
                        "dataset_name": dataset.get("dataset_name"),
                        "parquet_files": dataset.get("parquet_files"),
                        "bytes_on_disk": dataset.get("bytes_on_disk"),
                    }
                )
        if dataset_rows:
            st.markdown("#### Detalle físico por dataset")
            dataset_inventory = pd.DataFrame(dataset_rows)
            dataset_inventory["parquet_files"] = numeric_series(
                dataset_inventory, "parquet_files"
            )
            dataset_inventory["bytes_on_disk"] = numeric_series(
                dataset_inventory, "bytes_on_disk"
            )
            st.dataframe(dataset_inventory, width="stretch", hide_index=True)

        scan_errors: list[dict[str, object]] = []
        for row in physical.to_dict(orient="records"):
            details = row.get("scan_errors")
            if not isinstance(details, list):
                continue
            for error in details:
                if isinstance(error, dict):
                    scan_errors.append({"layer": row.get("layer"), **error})
        if scan_errors:
            st.warning("El inventario físico encontró rutas que no pudo inspeccionar.")
            st.dataframe(pd.DataFrame(scan_errors), width="stretch", hide_index=True)

    section_title("Historial de eventos físicos")
    if datasets.empty:
        empty_state("Sin eventos de datasets", "Las nuevas ejecuciones escribirán dataset_events.jsonl.")
    else:
        data = datasets.copy()
        data["parquet_files"] = numeric_series(data, "parquet_files")
        data["bytes_on_disk"] = numeric_series(data, "bytes_on_disk")
        grouped = (
            data.groupby(["layer", "status"], as_index=False)
            .agg(events=("dataset_name", "count"))
        )
        fig = px.bar(
            grouped,
            x="layer",
            y="events",
            color="status",
            barmode="stack",
            labels={"layer": "Capa", "events": "Eventos", "status": "Estado"},
            color_discrete_map=STATUS_COLORS,
        )
        st.plotly_chart(style_plotly(fig, height=370), width="stretch")
        shown = [
            column
            for column in (
                "recorded_at",
                "execution_id",
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
                "metadata.download_duration_seconds",
                "metadata.throughput_bytes_per_second",
                "error_type",
                "error_message",
            )
            if column in data.columns
        ]
        st.dataframe(data[shown], width="stretch", hide_index=True)

with tab_coverage:
    section_title("Datasets y periodos esperados")
    if coverage.empty:
        empty_state("Sin fotografías de cobertura", "Las ejecuciones escribirán coverage_snapshots.jsonl.")
    else:
        view = latest_coverage.copy()
        view["coverage_rate"] = pd.to_numeric(view.get("coverage_rate"), errors="coerce")
        fig = px.bar(
            view,
            x="layer",
            y="coverage_rate",
            color="status",
            range_y=[0, 1],
            labels={"layer": "Capa", "coverage_rate": "Cobertura", "status": "Estado"},
            color_discrete_map={
                "COMPLETE": "#8EDB8A",
                "PARTIAL": "#F2B35D",
                "NO_SCOPE": "#A7A7B3",
            },
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
        if len(coverage) > len(view):
            st.markdown("#### Historial de cobertura")
            history_columns = [column for column in shown if column in coverage.columns]
            st.dataframe(coverage[history_columns], width="stretch", hide_index=True)

with tab_quality:
    section_title("Reglas de calidad y reconciliación")
    if quality.empty and reconciliations.empty:
        empty_state("Sin resultados de calidad", "Las reglas se escribirán en quality_events.jsonl.")
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
                    "execution_id",
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
    section_title("Intentos HTTP y tiempos de descarga")
    if download_attempts.empty:
        empty_state("Sin intentos HTTP", "Bronze escribirá cada intento en download_attempts.jsonl.")
    else:
        left, right = st.columns(2)
        with left:
            attempt_summary = download_attempts.groupby(["service", "outcome"], as_index=False).size()
            fig = px.bar(
                attempt_summary,
                x="service",
                y="size",
                color="outcome",
                barmode="stack",
                labels={"service": "Servicio", "size": "Intentos", "outcome": "Resultado"},
            )
            st.plotly_chart(style_plotly(fig, height=360), width="stretch")
        with right:
            timing = download_attempts[download_attempts["duration_seconds"] > 0].copy()
            if timing.empty:
                empty_state("Sin tiempos", "Los registros antiguos pueden no contener duración.")
            else:
                fig = px.box(
                    timing,
                    x="service",
                    y="duration_seconds",
                    color="outcome",
                    points="all",
                    labels={"service": "Servicio", "duration_seconds": "Segundos", "outcome": "Resultado"},
                )
                st.plotly_chart(style_plotly(fig, height=360), width="stretch")
        shown = [
            column
            for column in (
                "attempted_at",
                "execution_id",
                "service",
                "year",
                "month",
                "attempt_number",
                "retry_number",
                "outcome",
                "status_code",
                "duration_seconds",
                "bytes_downloaded",
                "expected_bytes",
                "throughput_bytes_per_second",
                "retry_delay_seconds",
                "error_type",
                "error_message",
            )
            if column in download_attempts.columns
        ]
        st.dataframe(download_attempts[shown], width="stretch", hide_index=True)
    if not errors.empty:
        st.markdown("#### Errores consolidados")
        st.dataframe(errors, width="stretch", hide_index=True)

with tab_runs:
    section_title("Historial de corridas")
    if filtered_runs.empty:
        empty_state("Sin corridas", "No existen corridas para las capas seleccionadas.")
    else:
        shown = [
            column
            for column in (
                "started_at",
                "finished_at",
                "execution_id",
                "parent_execution_id",
                "layer",
                "execution_type",
                "status",
                "duration_seconds",
                "metrics.parquet_files_processed",
                "metrics.parquet_partitions_processed",
                "metrics.datasets_built",
                "metrics.models_trained",
                "metrics.error_rate",
                "error_type",
                "error_message",
            )
            if column in filtered_runs.columns
        ]
        st.dataframe(filtered_runs[shown], width="stretch", hide_index=True)
