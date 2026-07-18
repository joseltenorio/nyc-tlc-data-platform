from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import streamlit as st

from dashboard.config import load_dashboard_config, project_path


RUN_COLUMNS = [
    "execution_id",
    "layer",
    "execution_type",
    "status",
    "started_at",
    "finished_at",
    "duration_seconds",
    "source_files",
    "processed_files",
    "skipped_files",
    "failed_files",
    "rows_input",
    "rows_valid",
    "rows_rejected",
    "warning_rows",
    "datasets_built",
    "failed_datasets",
    "trained_models",
    "failed_models",
    "error_rate",
    "manifest_path",
]


def _parse_datetime(value: Any) -> pd.Timestamp | pd.NaT:
    if value in (None, ""):
        return pd.NaT
    return pd.to_datetime(value, utc=True, errors="coerce")


def _duration(started: Any, finished: Any, explicit: Any = None) -> float | None:
    if explicit is not None:
        try:
            return float(explicit)
        except (TypeError, ValueError):
            pass
    start = _parse_datetime(started)
    end = _parse_datetime(finished)
    if pd.isna(start) or pd.isna(end):
        return None
    return float((end - start).total_seconds())


def _infer_layer(path: Path, payload: dict[str, Any], summary: dict[str, Any]) -> str:
    declared = str(summary.get("layer") or payload.get("layer") or "").lower()
    if declared:
        if declared.startswith("silver"):
            return "silver"
        if declared.startswith("gold"):
            return "gold"
        if declared.startswith("ml"):
            return "ml"
        if declared.startswith("bronze"):
            return "bronze"
    parts = {part.lower() for part in path.parts}
    if "silver" in parts:
        return "silver"
    if "gold" in parts:
        return "gold"
    if "ml" in parts:
        return "ml"
    return "bronze"


def _first_number(summary: dict[str, Any], names: Iterable[str]) -> float | None:
    for name in names:
        value = summary.get(name)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _normalize_manifest(path: Path, root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else payload
    layer = _infer_layer(path, payload, summary)
    execution_id = str(
        summary.get("execution_id")
        or summary.get("run_id")
        or payload.get("execution_id")
        or path.stem
    )
    status = str(summary.get("status") or "UNKNOWN").upper()
    source_files = _first_number(summary, ("source_files", "expected_periods", "applicable_periods"))
    processed_files = _first_number(
        summary,
        ("processed_files", "ready_files", "downloaded_files", "datasets_built", "trained_models"),
    )
    failed_files = _first_number(summary, ("failed_files", "failed_datasets", "failed_models"))
    denominator = source_files
    if layer == "gold":
        denominator = (_first_number(summary, ("datasets_built",)) or 0) + (
            _first_number(summary, ("failed_datasets",)) or 0
        )
    elif layer == "ml":
        denominator = (_first_number(summary, ("trained_models",)) or 0) + (
            _first_number(summary, ("failed_models",)) or 0
        )
    error_rate = None
    if denominator and denominator > 0:
        error_rate = float((failed_files or 0) / denominator)

    row = {
        "execution_id": execution_id,
        "layer": layer,
        "execution_type": str(summary.get("execution_type") or "run"),
        "status": status,
        "started_at": _parse_datetime(summary.get("started_at") or payload.get("started_at")),
        "finished_at": _parse_datetime(
            summary.get("finished_at") or payload.get("finished_at") or payload.get("refreshed_at")
        ),
        "duration_seconds": _duration(
            summary.get("started_at") or payload.get("started_at"),
            summary.get("finished_at") or payload.get("finished_at") or payload.get("refreshed_at"),
            summary.get("duration_seconds"),
        ),
        "source_files": source_files,
        "processed_files": processed_files,
        "skipped_files": _first_number(summary, ("skipped_files",)),
        "failed_files": failed_files,
        "rows_input": _first_number(summary, ("rows_read", "source_rows", "parquet_num_rows")),
        "rows_valid": _first_number(summary, ("rows_valid",)),
        "rows_rejected": _first_number(summary, ("rows_rejected",)),
        "warning_rows": _first_number(summary, ("warning_rows",)),
        "datasets_built": _first_number(summary, ("datasets_built",)),
        "failed_datasets": _first_number(summary, ("failed_datasets",)),
        "trained_models": _first_number(summary, ("trained_models",)),
        "failed_models": _first_number(summary, ("failed_models",)),
        "error_rate": error_rate,
        "manifest_path": str(path.relative_to(root.parent)),
    }

    errors: list[dict[str, Any]] = []
    declared_errors = summary.get("errors") or payload.get("errors") or []
    if isinstance(declared_errors, list):
        for error in declared_errors:
            if not isinstance(error, dict):
                continue
            errors.append(
                {
                    "execution_id": execution_id,
                    "layer": layer,
                    "status": status,
                    "error_type": error.get("error_type") or error.get("type") or "UnknownError",
                    "error_message": error.get("error_message") or error.get("message") or "",
                    "subject": error.get("model_name") or error.get("dataset_name") or error.get("file_name"),
                    "occurred_at": row["finished_at"],
                    "source": "manifest",
                }
            )
    if summary.get("error_type") or summary.get("error_message"):
        errors.append(
            {
                "execution_id": execution_id,
                "layer": layer,
                "status": status,
                "error_type": summary.get("error_type") or "UnknownError",
                "error_message": summary.get("error_message") or "",
                "subject": None,
                "occurred_at": row["finished_at"],
                "source": "manifest",
            }
        )
    return row, errors


@dataclass(frozen=True)
class ManifestAuditData:
    runs: pd.DataFrame
    errors: pd.DataFrame


@st.cache_data(ttl=60, show_spinner=False)
def _load_manifests_cached(root_text: str, signature: tuple[int, int]) -> ManifestAuditData:
    del signature
    root = Path(root_text)
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    if root.is_dir():
        for path in sorted(root.rglob("*.json")):
            try:
                row, manifest_errors = _normalize_manifest(path, root)
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                continue
            rows.append(row)
            errors.extend(manifest_errors)
    runs = pd.DataFrame(rows, columns=RUN_COLUMNS)
    if not runs.empty:
        runs = runs.sort_values("started_at", ascending=False, na_position="last").reset_index(drop=True)
    error_frame = pd.DataFrame(errors)
    return ManifestAuditData(runs=runs, errors=error_frame)


class ManifestAuditRepository:
    def __init__(self) -> None:
        root = load_dashboard_config()["paths"]["manifests_root"]
        self.root = project_path(root)

    def load(self) -> ManifestAuditData:
        files = list(self.root.rglob("*.json")) if self.root.is_dir() else []
        signature = (
            len(files),
            max((path.stat().st_mtime_ns for path in files), default=0),
        )
        return _load_manifests_cached(str(self.root), signature)
