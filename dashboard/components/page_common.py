from __future__ import annotations

import streamlit as st

from dashboard.components.layout import note, render_sidebar_brand
from dashboard.components.styles import apply_theme
from dashboard.config import load_dashboard_config


def bootstrap_page() -> None:
    config = load_dashboard_config().get("app", {})
    st.set_page_config(
        page_title=str(config.get("title", "NYC TLC Mobility Intelligence")),
        page_icon=str(config.get("page_icon", "🚕")),
        layout=str(config.get("layout", "wide")),
        initial_sidebar_state="expanded",
    )
    apply_theme()
    render_sidebar_brand()


def render_partial_hvfhv_notice() -> None:
    scope = load_dashboard_config().get("analysis_scope", {})
    partial = scope.get("partial_services", {}).get("fhvhv", {})
    text = partial.get("note")
    if text:
        note(str(text), warning=True)
