from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard.components.filters import apply_scope, coverage_note, render_scope_filters
from dashboard.components.layout import empty_state, format_compact, kpi_card, note, render_page_header, section_title
from dashboard.components.page_common import bootstrap_page
from dashboard.components.styles import SERVICE_COLORS, style_plotly
from dashboard.data_access.parquet_repository import AnalyticsRepository, normalize_service_names, numeric


DAY_LABELS = {1: "Dom", 2: "Lun", 3: "Mar", 4: "Mié", 5: "Jue", 6: "Vie", 7: "Sáb"}

bootstrap_page()
render_page_header(
    "Perfil temporal y comportamiento",
    "Patrones por día, hora y servicio, con métricas operativas disponibles.",
    "Análisis descriptivo 3 de 3",
)
repo = AnalyticsRepository()
heat = normalize_service_names(numeric(repo.read_mart("time_heatmap"), ["trip_count", "average_duration_seconds"]))
operations = normalize_service_names(numeric(repo.read_mart("operational_efficiency"), ["trip_count", "average_duration_seconds", "p95_duration_seconds", "average_speed_mph"]))
executive = normalize_service_names(numeric(repo.read_mart("executive_monthly"), ["trip_count", "average_duration_seconds", "average_distance_miles", "average_speed_mph"]))

if heat.empty:
    empty_state("No existe mart_time_heatmap", "Ejecuta Gold para publicar los patrones horarios.")
    st.stop()
selection = render_scope_filters(executive if not executive.empty else heat, key="time")
coverage = coverage_note(selection)
if coverage:
    note(coverage, warning=True)

filtered_heat = heat[heat["service_type"].isin(selection.services)] if selection.services else heat
filtered_ops = apply_scope(operations, selection) if not operations.empty else operations
filtered_exec = apply_scope(executive, selection) if not executive.empty else executive
if filtered_heat.empty:
    empty_state("Sin datos temporales para la selección", "Selecciona servicios con cobertura disponible.")
    st.stop()

hourly = filtered_heat.groupby("hour_of_day", as_index=False)["trip_count"].sum().sort_values("trip_count", ascending=False)
peak_hour = int(hourly.iloc[0]["hour_of_day"])
daily = filtered_heat.groupby("day_of_week", as_index=False)["trip_count"].sum().sort_values("trip_count", ascending=False)
peak_day = DAY_LABELS.get(int(daily.iloc[0]["day_of_week"]), str(daily.iloc[0]["day_of_week"]))
weighted_duration = (
    (filtered_heat["average_duration_seconds"] * filtered_heat["trip_count"]).sum() / filtered_heat["trip_count"].sum()
    if filtered_heat["trip_count"].sum() else None
)

cols = st.columns(4)
with cols[0]:
    kpi_card("Hora pico", f"{peak_hour:02d}:00", format_compact(hourly.iloc[0]["trip_count"]))
with cols[1]:
    kpi_card("Día de mayor demanda", peak_day, format_compact(daily.iloc[0]["trip_count"]))
with cols[2]:
    kpi_card("Duración promedio", "—" if weighted_duration is None else f"{weighted_duration / 60:.1f} min", "Ponderada por viajes")
with cols[3]:
    speed = None
    if not filtered_exec.empty and filtered_exec["trip_count"].sum() > 0:
        speed = (filtered_exec["average_speed_mph"] * filtered_exec["trip_count"]).sum() / filtered_exec["trip_count"].sum()
    kpi_card("Velocidad promedio", "—" if speed is None else f"{speed:.1f} mph", "Según cobertura disponible")

left, right = st.columns([1.55, 1])
with left:
    section_title("Mapa de calor día × hora")
    matrix = filtered_heat.groupby(["day_of_week", "hour_of_day"], as_index=False)["trip_count"].sum()
    pivot = matrix.pivot(index="day_of_week", columns="hour_of_day", values="trip_count").fillna(0)
    pivot.index = [DAY_LABELS.get(int(value), str(value)) for value in pivot.index]
    figure = px.imshow(
        pivot,
        aspect="auto",
        color_continuous_scale=["#FFFFFF", "#8EDB8A", "#262538"],
        labels=dict(x="Hora", y="Día", color="Viajes"),
    )
    st.plotly_chart(style_plotly(figure, height=420), width="stretch")
with right:
    section_title("Demanda por hora y servicio")
    hourly_service = filtered_heat.groupby(["service_label", "hour_of_day"], as_index=False)["trip_count"].sum()
    figure = px.line(
        hourly_service,
        x="hour_of_day",
        y="trip_count",
        color="service_label",
        markers=True,
        color_discrete_map=SERVICE_COLORS,
        labels={"hour_of_day": "Hora", "trip_count": "Viajes", "service_label": "Servicio"},
    )
    st.plotly_chart(style_plotly(figure, height=420), width="stretch")

section_title("Eficiencia temporal")
if filtered_ops.empty:
    empty_state("Sin mart_operational_efficiency", "La sección aparecerá cuando Gold publique las métricas de eficiencia.")
else:
    figures = st.columns(2)
    by_hour = filtered_ops.groupby(["service_label", "hour_of_day"], as_index=False).agg(
        trip_count=("trip_count", "sum"),
        average_speed_mph=("average_speed_mph", "mean"),
        p95_duration_seconds=("p95_duration_seconds", "mean"),
    )
    with figures[0]:
        speed_fig = px.line(
            by_hour,
            x="hour_of_day",
            y="average_speed_mph",
            color="service_label",
            color_discrete_map=SERVICE_COLORS,
            labels={"hour_of_day": "Hora", "average_speed_mph": "Velocidad (mph)", "service_label": "Servicio"},
        )
        st.plotly_chart(style_plotly(speed_fig, height=340), width="stretch")
    with figures[1]:
        p95_fig = px.line(
            by_hour,
            x="hour_of_day",
            y=by_hour["p95_duration_seconds"] / 60,
            color="service_label",
            color_discrete_map=SERVICE_COLORS,
            labels={"hour_of_day": "Hora", "y": "Duración P95 (min)", "service_label": "Servicio"},
        )
        st.plotly_chart(style_plotly(p95_fig, height=340), width="stretch")
