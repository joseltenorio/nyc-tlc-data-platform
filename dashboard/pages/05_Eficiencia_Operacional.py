from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard.components.filters import apply_scope, coverage_note, render_scope_filters
from dashboard.components.layout import empty_state, kpi_card, note, render_page_header, section_title
from dashboard.components.page_common import bootstrap_page
from dashboard.components.styles import SERVICE_COLORS, style_plotly
from dashboard.data_access.parquet_repository import AnalyticsRepository, normalize_service_names, numeric


bootstrap_page()
render_page_header(
    "Eficiencia operacional",
    "Velocidad, duración, rutas lentas y señales de congestión.",
    "Análisis diagnóstico 2 de 3",
)
repo = AnalyticsRepository()
operations = normalize_service_names(
    numeric(repo.read_mart("operational_efficiency"), ["trip_count", "average_duration_seconds", "p95_duration_seconds", "average_speed_mph"])
)
routes = normalize_service_names(
    numeric(repo.read_mart("route_efficiency"), ["trip_count", "average_duration_seconds", "average_distance_miles", "average_speed_mph"])
)
congestion = normalize_service_names(
    numeric(repo.read_mart("zone_congestion"), ["trip_count", "average_speed_mph", "average_duration_seconds", "p95_duration_seconds", "congestion_index"])
)

if operations.empty:
    empty_state("No existe mart_operational_efficiency", "Ejecuta Gold para publicar los agregados de eficiencia.")
    st.stop()
selection = render_scope_filters(operations, key="efficiency")
filtered = apply_scope(operations, selection)
filtered_routes = apply_scope(routes, selection) if not routes.empty else routes
filtered_congestion = apply_scope(congestion, selection) if not congestion.empty else congestion
coverage = coverage_note(selection)
if coverage:
    note(coverage, warning=True)
if filtered.empty:
    empty_state("Sin métricas para la selección", "Cambia el periodo o los servicios.")
    st.stop()

trips = filtered["trip_count"].sum()
weighted_speed = (filtered["average_speed_mph"] * filtered["trip_count"]).sum() / trips if trips else None
weighted_duration = (filtered["average_duration_seconds"] * filtered["trip_count"]).sum() / trips if trips else None
weighted_p95 = (filtered["p95_duration_seconds"] * filtered["trip_count"]).sum() / trips if trips else None

cols = st.columns(4)
with cols[0]:
    kpi_card("Velocidad promedio", "—" if weighted_speed is None else f"{weighted_speed:.1f} mph", "Ponderada por viajes")
with cols[1]:
    kpi_card("Duración promedio", "—" if weighted_duration is None else f"{weighted_duration / 60:.1f} min", "Ponderada por viajes")
with cols[2]:
    kpi_card("Duración P95", "—" if weighted_p95 is None else f"{weighted_p95 / 60:.1f} min", "Cola operacional")
with cols[3]:
    slow_count = 0
    if not filtered_routes.empty and "average_speed_mph" in filtered_routes.columns:
        slow_count = int((filtered_routes["average_speed_mph"] < 8).sum())
    kpi_card("Rutas < 8 mph", str(slow_count), "Agregados OD")

left, right = st.columns(2)
with left:
    section_title("Velocidad por hora")
    hour = filtered.groupby(["service_label", "hour_of_day"], as_index=False).agg(
        trip_count=("trip_count", "sum"),
        average_speed_mph=("average_speed_mph", "mean"),
    )
    fig = px.line(
        hour,
        x="hour_of_day",
        y="average_speed_mph",
        color="service_label",
        markers=True,
        color_discrete_map=SERVICE_COLORS,
        labels={"hour_of_day": "Hora", "average_speed_mph": "Velocidad (mph)", "service_label": "Servicio"},
    )
    st.plotly_chart(style_plotly(fig, height=370), width="stretch")
with right:
    section_title("Duración P95 por hora")
    hour = hour.merge(
        filtered.groupby(["service_label", "hour_of_day"], as_index=False)["p95_duration_seconds"].mean(),
        on=["service_label", "hour_of_day"],
        how="left",
    )
    hour["p95_minutes"] = hour["p95_duration_seconds"] / 60
    fig = px.line(
        hour,
        x="hour_of_day",
        y="p95_minutes",
        color="service_label",
        markers=True,
        color_discrete_map=SERVICE_COLORS,
        labels={"hour_of_day": "Hora", "p95_minutes": "P95 (min)", "service_label": "Servicio"},
    )
    st.plotly_chart(style_plotly(fig, height=370), width="stretch")

section_title("Rutas con menor eficiencia")
if filtered_routes.empty:
    empty_state("Sin mart_route_efficiency", "La tabla aparecerá después de construir rutas Gold.")
else:
    route_table = filtered_routes.groupby(
        ["pickup_zone_name", "dropoff_zone_name", "service_label"],
        dropna=False,
        as_index=False,
    ).agg(
        trip_count=("trip_count", "sum"),
        average_speed_mph=("average_speed_mph", "mean"),
        average_duration_seconds=("average_duration_seconds", "mean"),
        average_distance_miles=("average_distance_miles", "mean"),
    )
    route_table = route_table[route_table["trip_count"] >= route_table["trip_count"].quantile(0.60)]
    route_table = route_table.sort_values(["average_speed_mph", "trip_count"], ascending=[True, False]).head(20)
    route_table["route"] = route_table["pickup_zone_name"].astype(str) + " → " + route_table["dropoff_zone_name"].astype(str)
    route_table["duration_minutes"] = route_table["average_duration_seconds"] / 60
    st.dataframe(
        route_table[["route", "service_label", "trip_count", "average_speed_mph", "duration_minutes", "average_distance_miles"]],
        width="stretch",
        hide_index=True,
        column_config={
            "trip_count": st.column_config.NumberColumn("Viajes", format="%,.0f"),
            "average_speed_mph": st.column_config.NumberColumn("Velocidad", format="%.1f mph"),
            "duration_minutes": st.column_config.NumberColumn("Duración", format="%.1f min"),
            "average_distance_miles": st.column_config.NumberColumn("Distancia", format="%.2f mi"),
        },
    )

section_title("Zonas con mayor índice de congestión")
if filtered_congestion.empty:
    empty_state("Sin mart_zone_congestion", "No hay índice territorial disponible.")
else:
    zone = filtered_congestion.groupby(["pickup_zone_name", "pickup_borough"], dropna=False, as_index=False).agg(
        trip_count=("trip_count", "sum"),
        congestion_index=("congestion_index", "mean"),
        average_speed_mph=("average_speed_mph", "mean"),
    )
    zone = zone[zone["trip_count"] >= zone["trip_count"].quantile(0.60)].nlargest(15, "congestion_index").sort_values("congestion_index")
    fig = px.bar(
        zone,
        x="congestion_index",
        y="pickup_zone_name",
        orientation="h",
        color="average_speed_mph",
        color_continuous_scale=["#FF826B", "#F2B35D", "#8EDB8A"],
        labels={"congestion_index": "Índice de congestión", "pickup_zone_name": "Zona", "average_speed_mph": "Velocidad"},
    )
    st.plotly_chart(style_plotly(fig, height=430), width="stretch")
