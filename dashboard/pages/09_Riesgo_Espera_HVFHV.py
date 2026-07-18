from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.components.layout import empty_state, kpi_card, note, render_page_header, section_title
from dashboard.components.page_common import bootstrap_page
from dashboard.components.styles import style_plotly
from dashboard.data_access.parquet_repository import AnalyticsRepository, numeric


bootstrap_page()
render_page_header(
    "Riesgo de espera HVFHV",
    "Clasificación de recogidas con espera superior a 10 minutos.",
    "Resultado ML 3 de 3",
)
repo = AnalyticsRepository()
prediction_name = repo.ml_dataset_name("wait_risk", "prediction_dataset")
metrics_name = repo.ml_dataset_name("wait_risk", "metrics_dataset")
importance_name = repo.ml_dataset_name("wait_risk", "importance_dataset")
predictions = numeric(repo.read_ml(prediction_name), ["request_to_pickup_seconds", "observed_excessive_wait", "predicted_excessive_wait", "risk_probability"])
metrics = numeric(repo.read_ml(metrics_name), ["metric_value"])
importance = numeric(repo.read_ml(importance_name), ["importance"])

if predictions.empty:
    empty_state(
        "Modelo no entrenado con la cobertura actual",
        "HVFHV solo dispone de 2023. Completa 2024-2025 y ejecuta ml-train --models wait-risk.",
    )
    note(
        "La ausencia de predicciones no se reemplaza con valores simulados. La página permanecerá como estado pendiente hasta que exista data/ml/hvfhv_wait_risk_predictions.",
        warning=True,
    )
    st.stop()

predictions["request_datetime"] = pd.to_datetime(predictions["request_datetime"], errors="coerce", utc=True)
companies = sorted(predictions.get("hvfhs_company_name", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
selected_companies = st.sidebar.multiselect("Plataforma", companies, default=companies, key="wait_company")
filtered = predictions[predictions["hvfhs_company_name"].astype(str).isin(selected_companies)] if selected_companies else predictions.iloc[0:0]
boroughs = sorted(filtered.get("pickup_borough", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
selected_boroughs = st.sidebar.multiselect("Borough", boroughs, default=boroughs, key="wait_borough")
if selected_boroughs:
    filtered = filtered[filtered["pickup_borough"].astype(str).isin(selected_boroughs)]
if filtered.empty:
    empty_state("Sin predicciones para la selección", "Cambia plataforma o borough.")
    st.stop()

metric_values = {}
if not metrics.empty and {"metric_name", "metric_value"}.issubset(metrics.columns):
    metric_values = dict(zip(metrics["metric_name"].astype(str), metrics["metric_value"]))
algorithm = str(metrics.iloc[0]["algorithm"]) if not metrics.empty and "algorithm" in metrics.columns else "—"

cols = st.columns(5)
with cols[0]:
    kpi_card("Modelo ganador", algorithm.upper(), "Clasificación binaria")
with cols[1]:
    high_risk = (filtered["risk_probability"] >= 0.60).mean()
    kpi_card("Alto riesgo", f"{high_risk:.1%}", "Probabilidad ≥ 0.60")
with cols[2]:
    kpi_card("Espera promedio", f"{filtered['request_to_pickup_seconds'].mean() / 60:.1f} min", "Solicitud a pickup")
with cols[3]:
    auc_pr = metric_values.get("auc_pr")
    kpi_card("AUC-PR", "—" if auc_pr is None else f"{float(auc_pr):.3f}", "Métrica principal")
with cols[4]:
    f1 = metric_values.get("f1")
    kpi_card("F1", "—" if f1 is None else f"{float(f1):.3f}", "Balance precisión-recall")

left, right = st.columns([1.3, 1])
with left:
    section_title("Riesgo por zona")
    zone = filtered.groupby(["pickup_zone_name", "pickup_borough"], dropna=False, as_index=False).agg(
        trips=("risk_probability", "size"),
        average_risk=("risk_probability", "mean"),
        average_wait_seconds=("request_to_pickup_seconds", "mean"),
    )
    zone = zone[zone["trips"] >= zone["trips"].quantile(0.50)].nlargest(15, "average_risk").sort_values("average_risk")
    fig = px.bar(
        zone,
        x="average_risk",
        y="pickup_zone_name",
        orientation="h",
        color="average_wait_seconds",
        color_continuous_scale=["#8EDB8A", "#F2B35D", "#FF826B"],
        labels={"average_risk": "Probabilidad de espera excesiva", "pickup_zone_name": "Zona", "average_wait_seconds": "Espera"},
    )
    fig.update_xaxes(tickformat=".0%")
    st.plotly_chart(style_plotly(fig, height=440), width="stretch")
with right:
    section_title("Distribución por nivel")
    levels = filtered.groupby("predicted_risk_level", as_index=False).size()
    fig = px.pie(
        levels,
        names="predicted_risk_level",
        values="size",
        hole=0.58,
        color="predicted_risk_level",
        color_discrete_map={"NORMAL": "#8EDB8A", "ELEVATED": "#F2B35D", "HIGH": "#FF9C79", "CRITICAL": "#F07867"},
    )
    st.plotly_chart(style_plotly(fig, height=440), width="stretch")

section_title("Importancia de variables")
if importance.empty:
    empty_state("Sin tabla de importancia", "El algoritmo ganador puede no publicar importancia o la salida no está disponible.")
else:
    feature_col = "feature_name" if "feature_name" in importance.columns else "feature"
    if feature_col in importance.columns:
        top = importance.nlargest(20, "importance").sort_values("importance")
        fig = px.bar(top, x="importance", y=feature_col, orientation="h", labels={"importance": "Importancia", feature_col: "Variable"})
        fig.update_traces(marker_color="#262538")
        st.plotly_chart(style_plotly(fig, height=430), width="stretch")
    else:
        st.dataframe(importance, width="stretch", hide_index=True)

section_title("Métricas y predicciones de mayor riesgo")
cols = st.columns([1, 1.35])
with cols[0]:
    st.dataframe(metrics, width="stretch", hide_index=True)
with cols[1]:
    display_columns = [column for column in ["request_datetime", "pickup_zone_name", "pickup_borough", "hvfhs_company_name", "request_to_pickup_seconds", "risk_probability", "predicted_risk_level"] if column in filtered.columns]
    st.dataframe(filtered.sort_values("risk_probability", ascending=False)[display_columns].head(30), width="stretch", hide_index=True)
