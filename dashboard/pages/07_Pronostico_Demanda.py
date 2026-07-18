from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.components.layout import empty_state, format_compact, kpi_card, note, render_page_header, section_title
from dashboard.components.page_common import bootstrap_page
from dashboard.components.styles import style_plotly
from dashboard.data_access.parquet_repository import AnalyticsRepository, normalize_service_names, numeric


bootstrap_page()
render_page_header(
    "Pronóstico de demanda",
    "Predicción horaria por zona y servicio, con intervalos y anomalías históricas.",
    "Resultado ML 1 de 3",
)
repo = AnalyticsRepository()
forecast_name = repo.ml_dataset_name("forecast", "prediction_dataset")
metrics_name = repo.ml_dataset_name("forecast", "metrics_dataset")
anomalies_name = repo.ml_dataset_name("forecast", "anomaly_dataset")
forecast = normalize_service_names(
    numeric(repo.read_ml(forecast_name), ["horizon_hour", "predicted_trip_count", "lower_bound", "upper_bound"])
)
metrics = numeric(repo.read_ml(metrics_name), ["metric_value"])
anomalies = normalize_service_names(
    numeric(repo.read_ml(anomalies_name), ["observed_trip_count", "expected_trip_count", "residual", "deviation_score"])
)

if forecast.empty:
    empty_state(
        "El modelo de pronóstico aún no tiene salida",
        "Ejecuta: docker compose run --rm pipeline ml-train --models forecast",
    )
    note("La página no genera predicciones ficticias. Solo muestra data/ml/demand_forecast cuando el entrenamiento finaliza.", warning=True)
    st.stop()

forecast["forecast_timestamp"] = pd.to_datetime(forecast["forecast_timestamp"], errors="coerce", utc=True)
services = sorted(forecast["service_type"].dropna().astype(str).unique().tolist())
selected_services = st.sidebar.multiselect("Servicio", services, default=services, key="forecast_services")
filtered = forecast[forecast["service_type"].isin(selected_services)] if selected_services else forecast.iloc[0:0]
boroughs = sorted(filtered.get("pickup_borough", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
selected_borough = st.sidebar.selectbox("Borough", ["Todos", *boroughs], key="forecast_borough")
if selected_borough != "Todos" and "pickup_borough" in filtered.columns:
    filtered = filtered[filtered["pickup_borough"].astype(str) == selected_borough]
zones = sorted(filtered.get("pickup_zone_name", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
selected_zone = st.sidebar.selectbox("Zona", ["Todas", *zones], key="forecast_zone")
if selected_zone != "Todas" and "pickup_zone_name" in filtered.columns:
    filtered = filtered[filtered["pickup_zone_name"].astype(str) == selected_zone]

if filtered.empty:
    empty_state("Sin predicciones para la selección", "Cambia servicio, borough o zona.")
    st.stop()

metric_values = {}
if not metrics.empty and {"metric_name", "metric_value"}.issubset(metrics.columns):
    metric_values = dict(zip(metrics["metric_name"].astype(str), metrics["metric_value"]))
model_algorithm = "—"
if not metrics.empty and "algorithm" in metrics.columns:
    model_algorithm = str(metrics.iloc[0]["algorithm"])

peak = filtered.loc[filtered["predicted_trip_count"].idxmax()]
cols = st.columns(5)
with cols[0]:
    kpi_card("Modelo ganador", model_algorithm.upper(), "Selección por WAPE/RMSE")
with cols[1]:
    kpi_card("Demanda prevista", format_compact(filtered["predicted_trip_count"].sum()), "Horizonte disponible")
with cols[2]:
    kpi_card("Pico previsto", format_compact(peak["predicted_trip_count"]), str(peak["forecast_timestamp"]))
with cols[3]:
    wape = metric_values.get("wape")
    kpi_card("WAPE test", "—" if wape is None else f"{float(wape):.2%}", "Menor es mejor")
with cols[4]:
    improvement = metric_values.get("wape_improvement_vs_weekly")
    kpi_card("Mejora vs baseline", "—" if improvement is None else f"{float(improvement):+.2%}", "Vs. lag semanal", color="#FF826B" if improvement is not None and float(improvement) < 0 else "#8EDB8A")

section_title("Pronóstico agregado con intervalo")
series = filtered.groupby("forecast_timestamp", as_index=False).agg(
    predicted_trip_count=("predicted_trip_count", "sum"),
    lower_bound=("lower_bound", "sum"),
    upper_bound=("upper_bound", "sum"),
)
figure = go.Figure()
figure.add_trace(go.Scatter(x=series["forecast_timestamp"], y=series["upper_bound"], mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"))
figure.add_trace(go.Scatter(x=series["forecast_timestamp"], y=series["lower_bound"], mode="lines", line=dict(width=0), fill="tonexty", fillcolor="rgba(142,219,138,0.25)", name="Intervalo 90%"))
figure.add_trace(go.Scatter(x=series["forecast_timestamp"], y=series["predicted_trip_count"], mode="lines+markers", line=dict(color="#262538", width=3), marker=dict(color="#FF826B"), name="Predicción"))
figure.update_xaxes(title="Hora")
figure.update_yaxes(title="Viajes previstos")
st.plotly_chart(style_plotly(figure, height=420), width="stretch")

left, right = st.columns([1.25, 1])
with left:
    section_title("Zonas con mayor demanda prevista")
    zone_rank = filtered.groupby(["pickup_zone_name", "pickup_borough"], dropna=False, as_index=False)["predicted_trip_count"].sum().nlargest(15, "predicted_trip_count").sort_values("predicted_trip_count")
    fig = px.bar(
        zone_rank,
        x="predicted_trip_count",
        y="pickup_zone_name",
        orientation="h",
        color="pickup_borough",
        labels={"predicted_trip_count": "Viajes previstos", "pickup_zone_name": "Zona", "pickup_borough": "Borough"},
    )
    st.plotly_chart(style_plotly(fig, height=430), width="stretch")
with right:
    section_title("Nivel de demanda")
    levels = filtered.groupby("demand_level", as_index=False)["predicted_trip_count"].sum()
    fig = px.pie(
        levels,
        names="demand_level",
        values="predicted_trip_count",
        hole=0.58,
        color="demand_level",
        color_discrete_map={"LOW": "#DFF4F0", "NORMAL": "#8EDB8A", "HIGH": "#F2B35D", "CRITICAL": "#FF826B"},
    )
    st.plotly_chart(style_plotly(fig, height=430), width="stretch")

section_title("Métricas del modelo")
if metrics.empty:
    empty_state("Sin demand_forecast_metrics", "El modelo tiene predicciones, pero no se encontró su tabla de métricas.")
else:
    metric_table = metrics[[column for column in ["model_name", "algorithm", "metric_name", "metric_value"] if column in metrics.columns]].copy()
    st.dataframe(metric_table, width="stretch", hide_index=True)

section_title("Anomalías históricas detectadas")
if anomalies.empty:
    empty_state("Sin demand_anomalies", "No hay anomalías persistidas para mostrar.")
else:
    anomaly_rows = anomalies[anomalies.get("anomaly_flag", False).astype(bool)] if "anomaly_flag" in anomalies.columns else anomalies
    if selected_services:
        anomaly_rows = anomaly_rows[anomaly_rows["service_type"].isin(selected_services)]
    anomaly_rows["event_timestamp"] = pd.to_datetime(anomaly_rows["event_timestamp"], errors="coerce", utc=True)
    st.dataframe(
        anomaly_rows.sort_values("deviation_score", key=lambda s: s.abs(), ascending=False).head(30),
        width="stretch",
        hide_index=True,
    )
