from __future__ import annotations

import html
from typing import Iterable

import streamlit as st

from dashboard.components.styles import STATUS_COLORS
from dashboard.config import load_dashboard_config


def render_sidebar_brand() -> None:
    st.sidebar.markdown(
        """
        <div class="tlc-brand">
          <div class="tlc-brand-mark">T</div>
          <div>
            <div class="tlc-brand-title">TLC Mobility</div>
            <div class="tlc-brand-subtitle">Data Intelligence Platform</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_page_header(title: str, subtitle: str, eyebrow: str) -> None:
    st.markdown(
        f"""
        <div class="tlc-page-header">
          <div>
            <div class="tlc-eyebrow">{html.escape(eyebrow)}</div>
            <h1 class="tlc-page-title">{html.escape(title)}</h1>
            <div class="tlc-page-subtitle">{html.escape(subtitle)}</div>
          </div>
          <div class="tlc-header-stripes"><span></span><span></span><span></span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def kpi_card(label: str, value: str, meta: str = "", *, color: str = "#8EDB8A") -> None:
    st.markdown(
        f"""
        <div class="tlc-kpi-card">
          <div class="tlc-kpi-title">{html.escape(label)}</div>
          <div class="tlc-kpi-body">
            <div class="tlc-kpi-value">{html.escape(value)}</div>
            <div class="tlc-kpi-meta"><span class="tlc-kpi-dot" style="background:{color}"></span>{html.escape(meta)}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_title(text: str) -> None:
    st.markdown(f'<div class="tlc-section-title">{html.escape(text)}</div>', unsafe_allow_html=True)


def note(text: str, *, warning: bool = False) -> None:
    classes = "tlc-note tlc-warning" if warning else "tlc-note"
    st.markdown(f'<div class="{classes}">{html.escape(text)}</div>', unsafe_allow_html=True)


def empty_state(title: str, detail: str) -> None:
    st.markdown(
        f'<div class="tlc-empty"><strong>{html.escape(title)}</strong><br>{html.escape(detail)}</div>',
        unsafe_allow_html=True,
    )


def status_badge(status: str) -> str:
    normalized = str(status or "UNKNOWN").upper()
    config = load_dashboard_config()
    label = config.get("status_labels", {}).get(normalized, normalized.replace("_", " ").title())
    color = STATUS_COLORS.get(normalized, STATUS_COLORS["UNKNOWN"])
    return (
        f'<span class="tlc-badge"><span class="tlc-badge-dot" style="background:{color}"></span>'
        f'{html.escape(label)}</span>'
    )


def render_badges(statuses: Iterable[str]) -> None:
    markup = " ".join(status_badge(status) for status in statuses)
    st.markdown(markup, unsafe_allow_html=True)


def format_compact(value: float | int | None, *, decimals: int = 1) -> str:
    if value is None:
        return "—"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    absolute = abs(number)
    for divisor, suffix in ((1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")):
        if absolute >= divisor:
            return f"{number / divisor:.{decimals}f}{suffix}"
    if number.is_integer():
        return f"{int(number):,}"
    return f"{number:,.{decimals}f}"


def format_duration(seconds: float | int | None) -> str:
    if seconds is None:
        return "—"
    try:
        total = max(0, int(float(seconds)))
    except (TypeError, ValueError):
        return "—"
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"
