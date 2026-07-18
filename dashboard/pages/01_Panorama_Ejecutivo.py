from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.components.filters import apply_scope, coverage_note, render_scope_filters
from dashboard.components.layout import empty_state, format_compact, kpi_card, note, render_page_header, section_title
from dashboard.components.page_common import bootstrap_page
from dashboard.components.styles import SERVICE_COLORS, style_plotly
from dashboard.data_access.parquet_repository import AnalyticsRepository, normalize_service_names, numeric


bootstrap_page()
render_page_header(
    "Panorama ejecutivo",
    "Volumen, tendencia y participación de los servicios TLC con cobertura Gold disponible.",
    "Análisis descriptivo 1 de 3",
)

repo = AnalyticsRepository()
executive = normalize_service_names(
    numeric(
        repo.read_mart("executive_monthly"),
        [
            "trip_count",
            "average_duration_seconds",
            "average_distance_miles",
            "average_speed_mph",
            "airport_trip_count",
            "quality_warning_count",
        ],
    )
)
daily = normalize_service_names(numeric(repo.read_mart("daily_demand"), ["trip_count"]))

if executive.empty:
    empty_state("No existe mart_executive_monthly", "Ejecuta gold-run con Yellow, Green y FHV 2023-2025.")
    st.stop()

selection = render_scope_filters(executive, key="overview")
filtered = apply_scope(executive, selection)
coverage = coverage_note(selection)
if coverage:
    note(coverage, warning=True)

if filtered.empty:
    empty_state("La selección no tiene datos", "Cambia los filtros. Las ausencias no se representan como cero.")
    st.stop()

def weighted(value_col: str):
    total_trips = filtered["trip_count"].sum()
    if value_col not in filtered.columns or total_trips <= 0:
        return None
    return (filtered[value_col] * filtered["trip_count"]).sum() / total_trips

previous = executive.copy()
period_count = filtered.groupby(["source_year", "source_month"], as_index=False)["trip_count"].sum()
period_count = period_count.sort_values(["source_year", "source_month"])
change = None
if len(period_count) >= 2:
    last, prior = period_count.iloc[-1]["trip_count"], period_count.iloc[-2]["trip_count"]
    if prior:
        change = (last - prior) / prior

cols = st.columns(5)
with cols[0]:
    kpi_card("Viajes", format_compact(filtered["trip_count"].sum()), "Selección actual")
with cols[1]:
    kpi_card("Variación último mes", "—" if change is None else f"{change:+.1%}", "Vs. mes anterior", color="#FF826B" if change is not None and change < 0 else "#8EDB8A")
with cols[2]:
    duration = weighted("average_duration_seconds")
    kpi_card("Duración promedio", "—" if duration is None else f"{duration / 60:.1f} min", "Ponderada por viajes")
with cols[3]:
    distance = weighted("average_distance_miles")
    kpi_card("Distancia promedio", "—" if distance is None else f"{distance:.2f} mi", "Cuando la fuente la reporta")
with cols[4]:
    speed = weighted("average_speed_mph")
    kpi_card("Velocidad promedio", "—" if speed is None else f"{speed:.1f} mph", "Promedio ponderado")

monthly = filtered.copy()
monthly["period"] = pd.to_datetime(dict(year=monthly["source_year"], month=monthly["source_month"], day=1))
left, right = st.columns([1.65, 1])
with left:
    section_title("Evolución mensual")
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
    section_title("Participación acumulada")
    share = filtered.groupby("service_label", as_index=False)["trip_count"].sum()
    fig = px.donut if hasattr(px, "donut") else None
    pie = px.pie(
        share,
        names="service_label",
        values="trip_count",
        hole=0.62,
        color="service_label",
        color_discrete_map=SERVICE_COLORS,
    )
    pie.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(style_plotly(pie, height=390), width="stretch")

section_title("Demanda diaria")
if daily.empty:
    empty_state("Sin mart_daily_demand", "La tendencia diaria aparecerá tras reconstruir los marts Gold.")
else:
    daily_filtered = daily.copy()
    daily_filtered["event_date"] = pd.to_datetime(daily_filtered["event_date"], errors="coerce")
    if selection.years:
        daily_filtered = daily_filtered[daily_filtered["event_date"].dt.year.isin(selection.years)]
    if selection.months:
        daily_filtered = daily_filtered[daily_filtered["event_date"].dt.month.isin(selection.months)]
    if selection.services:
        daily_filtered = daily_filtered[daily_filtered["service_type"].isin(selection.services)]
    line = px.line(
        daily_filtered.sort_values("event_date"),
        x="event_date",
        y="trip_count",
        color="service_label",
        color_discrete_map=SERVICE_COLORS,
        labels={"event_date": "Fecha", "trip_count": "Viajes", "service_label": "Servicio"},
    )
    st.plotly_chart(style_plotly(line, height=330), width="stretch")
