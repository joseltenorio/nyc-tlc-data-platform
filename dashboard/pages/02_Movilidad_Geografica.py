from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.components.filters import apply_scope, coverage_note, render_scope_filters
from dashboard.components.layout import empty_state, format_compact, kpi_card, note, render_page_header, section_title
from dashboard.components.page_common import bootstrap_page
from dashboard.components.styles import style_plotly
from dashboard.data_access.parquet_repository import AnalyticsRepository, normalize_service_names, numeric


bootstrap_page()
render_page_header(
    "Movilidad geográfica",
    "Zonas de origen, corredores principales y relaciones entre boroughs.",
    "Análisis descriptivo 2 de 3",
)
repo = AnalyticsRepository()
zones = normalize_service_names(
    numeric(repo.read_mart("zone_demand"), ["pickup_trip_count", "average_duration_seconds", "average_distance_miles", "average_speed_mph"])
)
routes = normalize_service_names(
    numeric(repo.read_mart("od_routes"), ["trip_count", "average_duration_seconds", "average_distance_miles", "average_speed_mph"])
)

if zones.empty:
    empty_state("No existe mart_zone_demand", "Ejecuta Gold para publicar agregados geográficos.")
    st.stop()
selection = render_scope_filters(zones, key="geo")
filtered_zones = apply_scope(zones, selection)
filtered_routes = apply_scope(routes, selection) if not routes.empty else routes
coverage = coverage_note(selection)
if coverage:
    note(coverage, warning=True)
if filtered_zones.empty:
    empty_state("Sin cobertura para la selección", "No se crean ceros artificiales para periodos o servicios ausentes.")
    st.stop()

zone_rank = filtered_zones.groupby(["pickup_zone_name", "pickup_borough"], dropna=False, as_index=False)["pickup_trip_count"].sum()
zone_rank = zone_rank.sort_values("pickup_trip_count", ascending=False)
borough_rank = filtered_zones.groupby("pickup_borough", dropna=False, as_index=False)["pickup_trip_count"].sum().sort_values("pickup_trip_count", ascending=False)
route_rank = pd.DataFrame()
if not filtered_routes.empty:
    route_rank = filtered_routes.groupby(
        ["pickup_zone_name", "dropoff_zone_name", "pickup_borough", "dropoff_borough"],
        dropna=False,
        as_index=False,
    ).agg(trip_count=("trip_count", "sum"), average_speed_mph=("average_speed_mph", "mean"))
    route_rank = route_rank.sort_values("trip_count", ascending=False)

cols = st.columns(4)
with cols[0]:
    top = zone_rank.iloc[0]
    kpi_card("Principal origen", str(top["pickup_zone_name"]), f"{format_compact(top['pickup_trip_count'])} viajes")
with cols[1]:
    borough = borough_rank.iloc[0]
    kpi_card("Borough líder", str(borough["pickup_borough"]), f"{format_compact(borough['pickup_trip_count'])} viajes")
with cols[2]:
    kpi_card("Zonas activas", str(zone_rank["pickup_zone_name"].nunique()), "Con pickups registrados")
with cols[3]:
    if route_rank.empty:
        kpi_card("Ruta principal", "—", "Sin mart OD")
    else:
        route = route_rank.iloc[0]
        kpi_card("Ruta principal", f"{route['pickup_zone_name']} → {route['dropoff_zone_name']}", format_compact(route["trip_count"]))

left, right = st.columns([1.2, 1])
with left:
    section_title("Top zonas de origen")
    top_zones = zone_rank.head(15).sort_values("pickup_trip_count")
    fig = px.bar(
        top_zones,
        x="pickup_trip_count",
        y="pickup_zone_name",
        orientation="h",
        color="pickup_borough",
        labels={"pickup_trip_count": "Viajes", "pickup_zone_name": "Zona", "pickup_borough": "Borough"},
    )
    st.plotly_chart(style_plotly(fig, height=470), width="stretch")
with right:
    section_title("Distribución por borough")
    fig = px.treemap(
        borough_rank,
        path=["pickup_borough"],
        values="pickup_trip_count",
        color="pickup_trip_count",
        color_continuous_scale=["#DFF4F0", "#8EDB8A", "#262538"],
    )
    fig.update_coloraxes(showscale=False)
    st.plotly_chart(style_plotly(fig, height=470), width="stretch")

section_title("Flujo origen-destino entre boroughs")
if filtered_routes.empty:
    empty_state("Sin rutas origen-destino", "El mart_od_routes no está materializado para esta selección.")
else:
    matrix = filtered_routes.groupby(["pickup_borough", "dropoff_borough"], dropna=False, as_index=False)["trip_count"].sum()
    pivot = matrix.pivot(index="pickup_borough", columns="dropoff_borough", values="trip_count").fillna(0)
    heatmap = px.imshow(
        pivot,
        text_auto=".2s",
        aspect="auto",
        color_continuous_scale=["#FFFFFF", "#8EDB8A", "#262538"],
        labels=dict(x="Borough de destino", y="Borough de origen", color="Viajes"),
    )
    st.plotly_chart(style_plotly(heatmap, height=420), width="stretch")

    section_title("Corredores con mayor volumen")
    table = route_rank.head(20).copy()
    table["ruta"] = table["pickup_zone_name"].astype(str) + " → " + table["dropoff_zone_name"].astype(str)
    st.dataframe(
        table[["ruta", "pickup_borough", "dropoff_borough", "trip_count", "average_speed_mph"]],
        width="stretch",
        hide_index=True,
        column_config={
            "trip_count": st.column_config.NumberColumn("Viajes", format="%,.0f"),
            "average_speed_mph": st.column_config.NumberColumn("Velocidad promedio", format="%.1f mph"),
        },
    )
