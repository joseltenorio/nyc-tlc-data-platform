from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.components.layout import empty_state, format_compact, kpi_card, render_page_header, section_title
from dashboard.components.page_common import bootstrap_page, render_partial_hvfhv_notice
from dashboard.components.styles import SERVICE_COLORS, style_plotly
from dashboard.data_access.audit_repository import AuditRepository
from dashboard.data_access.parquet_repository import AnalyticsRepository, normalize_service_names, numeric


bootstrap_page()
render_page_header(
    "NYC TLC Mobility Intelligence",
    "Centro analítico para movilidad, modelos predictivos y control del pipeline Medallion.",
    "Inicio",
)
render_partial_hvfhv_notice()

repository = AnalyticsRepository()
executive = normalize_service_names(
    numeric(
        repository.read_mart("executive_monthly"),
        ["trip_count", "average_duration_seconds", "average_distance_miles", "average_speed_mph"],
    )
)
audit = AuditRepository().load()

if executive.empty:
    total_trips = None
    services = 0
    years = 0
else:
    total_trips = executive["trip_count"].sum(min_count=1)
    services = executive["service_type"].nunique() if "service_type" in executive.columns else 0
    years = executive["source_year"].nunique() if "source_year" in executive.columns else 0

success_rate = None
latest_status = "Sin corridas"
if not audit.runs.empty:
    finished = audit.runs[audit.runs["status"].astype(str).str.upper() != "RUNNING"]
    if not finished.empty:
        success_rate = finished["status"].astype(str).str.upper().isin(["SUCCESS", "SUCCEEDED"]).mean()
    latest_status = str(audit.runs.iloc[0].get("status", "UNKNOWN"))

cols = st.columns(4)
with cols[0]:
    kpi_card("Viajes Gold", format_compact(total_trips), "Suma disponible en marts")
with cols[1]:
    kpi_card("Servicios con cobertura", str(services), "No se imputan ausencias")
with cols[2]:
    kpi_card("Años analizados", str(years), "Cobertura materializada")
with cols[3]:
    kpi_card(
        "Éxito de corridas",
        "—" if success_rate is None else f"{success_rate:.1%}",
        f"Último estado: {latest_status}",
        color="#FF826B" if latest_status.upper() == "FAILED" else "#8EDB8A",
    )

left, right = st.columns([1.65, 1])
with left:
    section_title("Evolución mensual por servicio")
    if executive.empty:
        empty_state("Gold todavía no está disponible", "Ejecuta gold-run para materializar los marts analíticos.")
    else:
        monthly = executive.copy()
        monthly["period"] = pd.to_datetime(
            dict(year=monthly["source_year"], month=monthly["source_month"], day=1), errors="coerce"
        )
        fig = px.line(
            monthly.sort_values("period"),
            x="period",
            y="trip_count",
            color="service_label",
            markers=True,
            color_discrete_map=SERVICE_COLORS,
            labels={"period": "Periodo", "trip_count": "Viajes", "service_label": "Servicio"},
        )
        st.plotly_chart(style_plotly(fig, height=390), width="stretch")
with right:
    section_title("Estado de la plataforma")
    if audit.runs.empty:
        empty_state("Sin manifiestos de ejecución", "El dashboard no fabrica corridas; aparecerán cuando existan manifiestos o MongoDB.")
    else:
        summary = (
            audit.runs.assign(status=audit.runs["status"].astype(str).str.upper())
            .groupby(["layer", "status"], as_index=False)
            .size()
        )
        fig = px.bar(
            summary,
            x="layer",
            y="size",
            color="status",
            barmode="stack",
            labels={"layer": "Capa", "size": "Corridas", "status": "Estado"},
            color_discrete_map={
                "SUCCESS": "#8EDB8A",
                "PARTIAL_SUCCESS": "#F2B35D",
                "FAILED": "#FF826B",
                "RUNNING": "#6E9FD0",
                "NO_INPUT": "#A7A7B3",
            },
        )
        st.plotly_chart(style_plotly(fig, height=390), width="stretch")

st.caption(f"Fuente de auditoría: {audit.source_note}. Los valores mostrados provienen de salidas Gold/ML y manifiestos reales.")
