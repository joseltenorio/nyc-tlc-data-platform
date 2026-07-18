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
    "Competencia y cobertura",
    "Participación de servicios, dominio territorial y diversidad de oferta.",
    "Análisis diagnóstico 3 de 3",
)
repo = AnalyticsRepository()
competition = normalize_service_names(
    numeric(repo.read_mart("service_competition"), ["pickup_trip_count", "zone_total_trip_count", "zone_service_share"])
)
share = normalize_service_names(
    numeric(repo.read_mart("service_share"), ["trip_count", "all_services_trip_count", "service_share"])
)
zones = normalize_service_names(numeric(repo.read_mart("zone_demand"), ["pickup_trip_count"]))

if competition.empty:
    empty_state("No existe mart_service_competition", "Ejecuta Gold para materializar participación territorial.")
    st.stop()
selection = render_scope_filters(competition, key="competition")
filtered = apply_scope(competition, selection)
share_filtered = apply_scope(share, selection) if not share.empty else share
zone_filtered = apply_scope(zones, selection) if not zones.empty else zones
coverage = coverage_note(selection)
if coverage:
    note(coverage, warning=True)
if filtered.empty:
    empty_state("Sin datos para la selección", "No se interpreta la ausencia de HVFHV 2024-2025 como participación cero.")
    st.stop()

service_totals = filtered.groupby("service_label", as_index=False)["pickup_trip_count"].sum().sort_values("pickup_trip_count", ascending=False)
zone_service = filtered.groupby(["pickup_zone_key", "pickup_zone_name", "pickup_borough", "service_label"], dropna=False, as_index=False)["pickup_trip_count"].sum()
dominant_index = zone_service.groupby("pickup_zone_key")["pickup_trip_count"].idxmax()
dominant = zone_service.loc[dominant_index].copy()
diversity = zone_service.groupby("pickup_zone_key")["service_label"].nunique()

cols = st.columns(4)
with cols[0]:
    leader = service_totals.iloc[0]
    kpi_card("Servicio líder", str(leader["service_label"]), format_compact(leader["pickup_trip_count"]))
with cols[1]:
    kpi_card("Zonas evaluadas", str(dominant["pickup_zone_key"].nunique()), "Con actividad Gold")
with cols[2]:
    multi = int((diversity >= 3).sum())
    kpi_card("Zonas multimodales", str(multi), "Tres o más servicios")
with cols[3]:
    concentrated = dominant.groupby("service_label").size().max() / len(dominant) if len(dominant) else None
    kpi_card("Concentración territorial", "—" if concentrated is None else f"{concentrated:.1%}", "Zonas dominadas por un servicio")

left, right = st.columns([1.35, 1])
with left:
    section_title("Participación mensual")
    if share_filtered.empty:
        empty_state("Sin mart_service_share", "No hay tendencia de participación mensual.")
    else:
        trend = share_filtered.copy()
        trend["period"] = pd.to_datetime(dict(year=trend["source_year"], month=trend["source_month"], day=1))
        fig = px.area(
            trend.sort_values("period"),
            x="period",
            y="service_share",
            color="service_label",
            groupnorm="fraction",
            color_discrete_map=SERVICE_COLORS,
            labels={"period": "Periodo", "service_share": "Participación", "service_label": "Servicio"},
        )
        fig.update_yaxes(tickformat=".0%")
        st.plotly_chart(style_plotly(fig, height=410), width="stretch")
with right:
    section_title("Zonas dominadas por servicio")
    dominant_summary = dominant.groupby("service_label", as_index=False).size()
    fig = px.bar(
        dominant_summary,
        x="service_label",
        y="size",
        color="service_label",
        color_discrete_map=SERVICE_COLORS,
        labels={"service_label": "Servicio", "size": "Zonas"},
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(style_plotly(fig, height=410), width="stretch")

section_title("Dominio territorial y presión competitiva")
zone_pivot = zone_service.pivot_table(
    index=["pickup_zone_key", "pickup_zone_name", "pickup_borough"],
    columns="service_label",
    values="pickup_trip_count",
    aggfunc="sum",
    fill_value=0,
).reset_index()
service_columns = [column for column in SERVICE_COLORS if column in zone_pivot.columns and column[0].isupper()]
if service_columns:
    zone_pivot["total"] = zone_pivot[service_columns].sum(axis=1)
    for column in service_columns:
        zone_pivot[f"{column}_share"] = zone_pivot[column] / zone_pivot["total"].replace(0, pd.NA)
    zone_pivot["dominant_service"] = zone_pivot[service_columns].idxmax(axis=1)
    zone_pivot["dominant_share"] = zone_pivot[[f"{column}_share" for column in service_columns]].max(axis=1)
    zone_pivot = zone_pivot.sort_values(["dominant_share", "total"], ascending=[False, False])
    st.dataframe(
        zone_pivot[["pickup_zone_name", "pickup_borough", "dominant_service", "dominant_share", "total"]].head(30),
        width="stretch",
        hide_index=True,
        column_config={
            "dominant_share": st.column_config.ProgressColumn("Participación dominante", min_value=0.0, max_value=1.0, format="%.1%%"),
            "total": st.column_config.NumberColumn("Viajes", format="%,.0f"),
        },
    )
else:
    empty_state("Sin columnas de servicio", "La selección no permite calcular dominio territorial.")
