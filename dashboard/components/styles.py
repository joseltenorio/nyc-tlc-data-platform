from __future__ import annotations


import streamlit as st

from dashboard.config import PROJECT_ROOT


NAVY = "#262538"
CORAL = "#FF826B"
GREEN = "#8EDB8A"
MINT = "#DFF4F0"
TEXT = "#20202B"
MUTED = "#72727F"
GRID = "#E8E8EE"
SURFACE = "#FFFFFF"
BACKGROUND = "#F4FBFA"

SERVICE_COLORS = {
    "Yellow": "#E4B836",
    "Green": "#68B984",
    "FHV": "#6F7F96",
    "HVFHV": "#FF826B",
    "yellow": "#E4B836",
    "green": "#68B984",
    "fhv": "#6F7F96",
    "fhvhv": "#FF826B",
}

STATUS_COLORS = {
    "SUCCESS": "#6FCB83",
    "SUCCEEDED": "#6FCB83",
    "PARTIAL_SUCCESS": "#F2B35D",
    "FAILED": "#F07867",
    "RUNNING": "#6E9FD0",
    "NO_INPUT": "#A7A7B3",
    "NO_SCOPE": "#A7A7B3",
    "BLOCKED": "#A7A7B3",
    "READY": "#6FCB83",
    "SKIPPED": "#A7A7B3",
    "UNKNOWN": "#A7A7B3",
}


def apply_theme() -> None:
    css_path = PROJECT_ROOT / "dashboard" / "assets" / "styles.css"
    css = css_path.read_text(encoding="utf-8")
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


def style_plotly(fig, *, height: int = 380, legend_orientation: str = "h"):
    fig.update_layout(
        template="simple_white",
        height=height,
        margin=dict(l=24, r=24, t=52, b=24),
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        font=dict(family="Inter, Segoe UI, Arial, sans-serif", color=TEXT, size=12),
        title_font=dict(size=16, color=TEXT),
        legend=dict(
            orientation=legend_orientation,
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            bgcolor="rgba(255,255,255,0)",
        ),
        hoverlabel=dict(bgcolor=NAVY, font_color="white"),
    )
    fig.update_xaxes(showgrid=False, linecolor=GRID, tickfont_color=MUTED, title_font_color=MUTED)
    fig.update_yaxes(gridcolor=GRID, zeroline=False, tickfont_color=MUTED, title_font_color=MUTED)
    return fig
