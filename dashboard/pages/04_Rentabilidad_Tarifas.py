from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.components.filters import apply_scope, render_scope_filters
from dashboard.components.layout import empty_state, format_compact, kpi_card, note, render_page_header, section_title
from dashboard.components.page_common import bootstrap_page
from dashboard.components.styles import SERVICE_COLORS, style_plotly
from dashboard.data_access.parquet_repository import AnalyticsRepository, normalize_service_names, numeric


bootstrap_page()
render_page_header(
    "Rentabilidad, tarifas y propinas",
    "Factores asociados al ingreso observable en Yellow y Green.",
    "Análisis diagnóstico 1 de 3",
)
repo = AnalyticsRepository()
financial = normalize_service_names(
    numeric(
        repo.read_mart("financial_profile"),
        ["trip_count", "fare_amount", "tip_amount", "tolls_amount", "total_amount", "average_total_amount", "average_tip_percentage"],
    )
)
profit = normalize_service_names(
    numeric(
        repo.read_mart("profitability_drivers"),
        ["trip_count", "average_total_amount", "average_fare_per_mile", "average_revenue_per_minute", "average_tip_percentage"],
    )
)
tips = normalize_service_names(
    numeric(
        repo.read_mart("tip_behavior"),
        ["credit_card_trip_count", "average_tip_amount", "average_tip_percentage", "median_tip_percentage"],
    )
)

if financial.empty:
    empty_state("No existe mart_financial_profile", "Este análisis requiere Yellow o Green procesados en Gold.")
    st.stop()
selection = render_scope_filters(financial, key="finance")
filtered = apply_scope(financial, selection)
profit_filtered = apply_scope(profit, selection) if not profit.empty else profit
tips_filtered = apply_scope(tips, selection) if not tips.empty else tips

non_taxi = [service for service in selection.services if service not in {"yellow", "green"}]
if non_taxi:
    note("Las métricas financieras comparables solo existen para Yellow y Green; FHV/HVFHV se excluyen de esta página.", warning=True)
if filtered.empty:
    empty_state("Sin información financiera para la selección", "Selecciona Yellow o Green.")
    st.stop()

trips = filtered["trip_count"].sum()
weighted_average = (
    (filtered["average_total_amount"] * filtered["trip_count"]).sum() / trips if trips else None
)
weighted_tip = (
    (filtered["average_tip_percentage"] * filtered["trip_count"]).sum() / trips if trips else None
)

cols = st.columns(5)
with cols[0]:
    kpi_card("Viajes financieros", format_compact(trips), "Yellow + Green")
with cols[1]:
    kpi_card("Ingresos observados", f"${format_compact(filtered['total_amount'].sum())}", "Total amount")
with cols[2]:
    kpi_card("Ticket promedio", "—" if weighted_average is None else f"${weighted_average:.2f}", "Ponderado por viajes")
with cols[3]:
    kpi_card("Propina promedio", "—" if weighted_tip is None else f"{weighted_tip:.1f}%", "Principalmente tarjetas")
with cols[4]:
    toll_share = filtered["tolls_amount"].sum() / filtered["total_amount"].sum() if filtered["total_amount"].sum() else None
    kpi_card("Peajes / ingreso", "—" if toll_share is None else f"{toll_share:.1%}", "Composición observada")

monthly = filtered.copy()
monthly["period"] = pd.to_datetime(dict(year=monthly["source_year"], month=monthly["source_month"], day=1))
left, right = st.columns([1.4, 1])
with left:
    section_title("Ingresos y ticket promedio")
    income = px.bar(
        monthly.sort_values("period"),
        x="period",
        y="total_amount",
        color="service_label",
        color_discrete_map=SERVICE_COLORS,
        barmode="group",
        labels={"period": "Periodo", "total_amount": "Ingreso", "service_label": "Servicio"},
    )
    st.plotly_chart(style_plotly(income, height=390), width="stretch")
with right:
    section_title("Composición financiera")
    composition = pd.DataFrame(
        {
            "componente": ["Tarifa", "Propina", "Peajes"],
            "monto": [filtered["fare_amount"].sum(), filtered["tip_amount"].sum(), filtered["tolls_amount"].sum()],
        }
    )
    fig = px.pie(
        composition,
        names="componente",
        values="monto",
        hole=0.58,
        color="componente",
        color_discrete_map={"Tarifa": "#262538", "Propina": "#8EDB8A", "Peajes": "#FF826B"},
    )
    st.plotly_chart(style_plotly(fig, height=390), width="stretch")

section_title("Factores de rentabilidad por zona")
if profit_filtered.empty:
    empty_state("Sin mart_profitability_drivers", "Reconstruye los marts Gold para habilitar el diagnóstico territorial.")
else:
    zones = profit_filtered.groupby(["pickup_zone_name", "pickup_borough"], dropna=False, as_index=False).agg(
        trip_count=("trip_count", "sum"),
        average_total_amount=("average_total_amount", "mean"),
        average_fare_per_mile=("average_fare_per_mile", "mean"),
        average_revenue_per_minute=("average_revenue_per_minute", "mean"),
        average_tip_percentage=("average_tip_percentage", "mean"),
    )
    zones = zones[zones["trip_count"] >= zones["trip_count"].quantile(0.50)]
    scatter = px.scatter(
        zones,
        x="average_fare_per_mile",
        y="average_revenue_per_minute",
        size="trip_count",
        color="average_tip_percentage",
        hover_name="pickup_zone_name",
        hover_data=["pickup_borough", "average_total_amount"],
        color_continuous_scale=["#DFF4F0", "#8EDB8A", "#FF826B"],
        labels={
            "average_fare_per_mile": "Ingreso por milla",
            "average_revenue_per_minute": "Ingreso por minuto",
            "average_tip_percentage": "% propina",
        },
    )
    st.plotly_chart(style_plotly(scatter, height=420), width="stretch")

section_title("Comportamiento de propina con tarjeta")
if tips_filtered.empty:
    empty_state("Sin mart_tip_behavior", "La fuente no contiene agregados de propina para la selección.")
else:
    by_hour = tips_filtered.groupby("time_key", as_index=False).agg(
        average_tip_percentage=("average_tip_percentage", "mean"),
        median_tip_percentage=("median_tip_percentage", "mean"),
        credit_card_trip_count=("credit_card_trip_count", "sum"),
    )
    fig = px.line(
        by_hour.sort_values("time_key"),
        x="time_key",
        y=["average_tip_percentage", "median_tip_percentage"],
        markers=True,
        labels={"time_key": "Hora", "value": "Propina (%)", "variable": "Métrica"},
        color_discrete_sequence=["#FF826B", "#262538"],
    )
    st.plotly_chart(style_plotly(fig, height=340), width="stretch")
