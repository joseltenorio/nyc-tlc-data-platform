from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.components.layout import empty_state, kpi_card, note, render_page_header, section_title
from dashboard.components.page_common import bootstrap_page
from dashboard.components.styles import style_plotly
from dashboard.data_access.parquet_repository import AnalyticsRepository, numeric


bootstrap_page()
render_page_header(
    "Segmentación de zonas",
    "Perfiles territoriales derivados con K-Means y selección de k mediante silhouette.",
    "Resultado ML 2 de 3",
)
repo = AnalyticsRepository()
assignments_name = repo.ml_dataset_name("segmentation", "assignment_dataset")
profiles_name = repo.ml_dataset_name("segmentation", "profile_dataset")
metrics_name = repo.ml_dataset_name("segmentation", "metrics_dataset")
assignments = numeric(
    repo.read_ml(assignments_name),
    [
        "cluster_id",
        "total_pickups",
        "total_dropoffs",
        "average_duration_seconds",
        "average_distance_miles",
        "average_speed_mph",
        "night_trip_share",
        "weekend_trip_share",
        "airport_trip_share",
        "service_diversity",
        "demand_coefficient_of_variation",
    ],
)
profiles = numeric(repo.read_ml(profiles_name), ["cluster_id", "zone_count", "total_pickups", "night_trip_share", "weekend_trip_share", "airport_trip_share", "service_diversity", "demand_coefficient_of_variation"])
metrics = numeric(repo.read_ml(metrics_name), ["metric_value"])

if assignments.empty:
    empty_state(
        "La segmentación aún no tiene salida",
        "Ejecuta: docker compose run --rm pipeline ml-train --models segmentation",
    )
    note("No se crean clusters ficticios. La página se habilita con data/ml/zone_segments.", warning=True)
    st.stop()

boroughs = sorted(assignments.get("borough", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
selected_boroughs = st.sidebar.multiselect("Borough", boroughs, default=boroughs, key="segments_borough")
filtered = assignments[assignments["borough"].astype(str).isin(selected_boroughs)] if selected_boroughs else assignments.iloc[0:0]
segments = sorted(filtered.get("segment_name", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
selected_segments = st.sidebar.multiselect("Segmento", segments, default=segments, key="segments_name")
if selected_segments:
    filtered = filtered[filtered["segment_name"].astype(str).isin(selected_segments)]

metric_values = {}
if not metrics.empty and {"metric_name", "metric_value"}.issubset(metrics.columns):
    metric_values = dict(zip(metrics["metric_name"].astype(str), metrics["metric_value"]))
best_k = metric_values.get("best_k")
silhouette = metric_values.get("silhouette")

cols = st.columns(4)
with cols[0]:
    kpi_card("Número de segmentos", "—" if best_k is None else str(int(float(best_k))), "k seleccionado")
with cols[1]:
    kpi_card("Silhouette", "—" if silhouette is None else f"{float(silhouette):.3f}", "Separación de clusters")
with cols[2]:
    kpi_card("Zonas segmentadas", str(filtered["zone_key"].nunique()), "Cobertura disponible")
with cols[3]:
    largest = filtered.groupby("segment_name").size().sort_values(ascending=False)
    kpi_card("Segmento principal", str(largest.index[0]) if not largest.empty else "—", f"{int(largest.iloc[0]) if not largest.empty else 0} zonas")

left, right = st.columns([1.1, 1])
with left:
    section_title("Tamaño de los segmentos")
    segment_size = filtered.groupby("segment_name", as_index=False).agg(zone_count=("zone_key", "nunique"), total_pickups=("total_pickups", "sum"))
    fig = px.bar(
        segment_size.sort_values("zone_count"),
        x="zone_count",
        y="segment_name",
        orientation="h",
        color="total_pickups",
        color_continuous_scale=["#DFF4F0", "#8EDB8A", "#262538"],
        labels={"zone_count": "Zonas", "segment_name": "Segmento", "total_pickups": "Pickups"},
    )
    st.plotly_chart(style_plotly(fig, height=420), width="stretch")
with right:
    section_title("Selección de k")
    silhouette_rows = []
    for name, value in metric_values.items():
        if name.startswith("silhouette_k_"):
            silhouette_rows.append({"k": int(name.rsplit("_", 1)[-1]), "silhouette": float(value)})
    if not silhouette_rows:
        empty_state("Sin detalle por k", "La métrica global está disponible, pero no la comparación de candidatos.")
    else:
        candidate_frame = pd.DataFrame(silhouette_rows).sort_values("k")
        fig = px.line(candidate_frame, x="k", y="silhouette", markers=True, labels={"k": "Número de clusters", "silhouette": "Silhouette"})
        fig.update_traces(line_color="#262538", marker_color="#FF826B", marker_size=10)
        st.plotly_chart(style_plotly(fig, height=420), width="stretch")

section_title("Perfil comparado de los segmentos")
profile_source = profiles if not profiles.empty else filtered.groupby(["cluster_id", "segment_name"], as_index=False).mean(numeric_only=True)
radar_metrics = [column for column in ["night_trip_share", "weekend_trip_share", "airport_trip_share", "service_diversity", "demand_coefficient_of_variation"] if column in profile_source.columns]
if not radar_metrics or profile_source.empty:
    empty_state("Sin perfiles de cluster", "No se encontraron centroides o promedios interpretables.")
else:
    normalized = profile_source.copy()
    for column in radar_metrics:
        minimum, maximum = normalized[column].min(), normalized[column].max()
        normalized[column] = 0.5 if maximum == minimum else (normalized[column] - minimum) / (maximum - minimum)
    figure = go.Figure()
    for _, row in normalized.iterrows():
        label = str(row.get("segment_name") or f"Cluster {int(row['cluster_id'])}")
        values = [float(row[column]) for column in radar_metrics]
        figure.add_trace(go.Scatterpolar(r=values + [values[0]], theta=radar_metrics + [radar_metrics[0]], fill="toself", name=label, opacity=0.55))
    figure.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 1])), showlegend=True)
    st.plotly_chart(style_plotly(figure, height=500, legend_orientation="v"), width="stretch")

section_title("Zonas y asignación")
columns = [column for column in ["location_id", "zone_name", "borough", "segment_name", "total_pickups", "total_dropoffs", "average_duration_seconds", "average_speed_mph", "night_trip_share", "airport_trip_share"] if column in filtered.columns]
st.dataframe(filtered[columns].sort_values(["segment_name", "total_pickups"], ascending=[True, False]), width="stretch", hide_index=True)
