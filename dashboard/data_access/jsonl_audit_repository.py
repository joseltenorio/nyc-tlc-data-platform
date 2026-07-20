from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import streamlit as st

from dashboard.config import audit_config, load_dashboard_config, project_path


@dataclass(frozen=True)
class JsonlAuditData:
    runs: pd.DataFrame
    attempts: pd.DataFrame
    quality: pd.DataFrame
    datasets: pd.DataFrame
    coverage: pd.DataFrame
    inventory: pd.DataFrame


def _is_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    return True


def _deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in patch.items():
        if not _is_present(value):
            continue
        if (
            key == "layer"
            and str(value).strip().lower() in {"", "unknown"}
            and str(result.get(key, "")).strip().lower() not in {"", "unknown"}
        ):
            continue
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _read_jsonl(path: Path, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                text = line.strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    payload["_source_file"] = str(path)
                    payload["_source_line"] = line_number
                    rows.append(payload)
    except OSError:
        return []
    if limit > 0 and len(rows) > limit:
        return rows[-limit:]
    return rows


def _records_for(root: Path, file_name: str, limit: int) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not root.is_dir():
        return records
    for path in sorted(root.glob(f"*/{file_name}")):
        records.extend(_read_jsonl(path, limit))
    records.sort(
        key=lambda item: (
            str(item.get("written_at") or item.get("checked_at") or item.get("recorded_at") or ""),
            str(item.get("_source_file") or ""),
            int(item.get("_source_line") or 0),
        )
    )
    if limit > 0 and len(records) > limit:
        records = records[-limit:]
    return records


def _coalesce_runs(records: Iterable[dict[str, Any]]) -> pd.DataFrame:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for record in records:
        execution_id = str(record.get("execution_id") or "").strip()
        if not execution_id:
            continue
        if execution_id not in merged:
            order.append(execution_id)
            merged[execution_id] = {}
        merged[execution_id] = _deep_merge(merged[execution_id], record)
    rows = [merged[execution_id] for execution_id in order]
    frame = pd.json_normalize(rows, sep=".") if rows else pd.DataFrame()
    if frame.empty:
        return frame
    if "run_id" in frame.columns and "execution_id" not in frame.columns:
        frame = frame.rename(columns={"run_id": "execution_id"})
    for column in ("started_at", "finished_at", "updated_at", "written_at"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    if "duration_seconds" not in frame.columns:
        frame["duration_seconds"] = pd.NA
    if {"started_at", "finished_at"}.issubset(frame.columns):
        calculated = (frame["finished_at"] - frame["started_at"]).dt.total_seconds()
        frame["duration_seconds"] = pd.to_numeric(
            frame["duration_seconds"], errors="coerce"
        ).fillna(calculated)
    return frame.sort_values("started_at", ascending=False, na_position="last").reset_index(drop=True)


def _to_frame(
    records: list[dict[str, Any]],
    *,
    dedupe_keys: list[str] | None = None,
    time_columns: tuple[str, ...] = (),
    sort_column: str | None = None,
) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    frame = pd.json_normalize(records, sep=".")
    for column in time_columns:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    if dedupe_keys and all(key in frame.columns for key in dedupe_keys):
        sort_candidates = [
            column
            for column in ("written_at", "updated_at", sort_column)
            if column and column in frame.columns
        ]
        if sort_candidates:
            frame = frame.sort_values(sort_candidates, na_position="last")
        frame = frame.drop_duplicates(dedupe_keys, keep="last")
    if sort_column and sort_column in frame.columns:
        frame = frame.sort_values(sort_column, ascending=False, na_position="last")
    return frame.reset_index(drop=True)


def _load_inventory(root: Path, current_file: str) -> pd.DataFrame:
    path = root / "inventory" / current_file
    if not path.is_file():
        return pd.DataFrame()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return pd.DataFrame()
    layers = payload.get("layers") if isinstance(payload, dict) else None
    if not isinstance(layers, list):
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for layer in layers:
        if not isinstance(layer, dict):
            continue
        row = {
            **layer,
            "snapshot_id": payload.get("snapshot_id"),
            "execution_id": payload.get("execution_id"),
            "trigger_layer": payload.get("trigger_layer"),
            "snapshot_status": payload.get("status"),
            "captured_at": payload.get("captured_at"),
        }
        rows.append(row)
    frame = pd.json_normalize(rows, sep=".") if rows else pd.DataFrame()
    if "captured_at" in frame.columns:
        frame["captured_at"] = pd.to_datetime(frame["captured_at"], utc=True, errors="coerce")
    if "latest_modified_at" in frame.columns:
        frame["latest_modified_at"] = pd.to_datetime(
            frame["latest_modified_at"], utc=True, errors="coerce"
        )
    return frame


@st.cache_data(ttl=60, show_spinner=False)
def _load_jsonl_cached(
    root_text: str,
    signature: tuple[int, int, int],
    settings: tuple[str, ...],
    limit: int,
) -> JsonlAuditData:
    del signature
    (
        pipeline_runs_file,
        dataset_events_file,
        quality_events_file,
        coverage_snapshots_file,
        download_attempts_file,
        inventory_current_file,
    ) = settings
    root = Path(root_text)
    runs = _coalesce_runs(_records_for(root, pipeline_runs_file, limit))
    datasets = _to_frame(
        _records_for(root, dataset_events_file, limit),
        dedupe_keys=["event_id"],
        time_columns=("recorded_at", "written_at"),
        sort_column="recorded_at",
    )
    quality = _to_frame(
        _records_for(root, quality_events_file, limit),
        dedupe_keys=["quality_id"],
        time_columns=("checked_at", "written_at"),
        sort_column="checked_at",
    )
    coverage = _to_frame(
        _records_for(root, coverage_snapshots_file, limit),
        dedupe_keys=["execution_id", "layer"],
        time_columns=("checked_at", "written_at"),
        sort_column="checked_at",
    )
    attempts = _to_frame(
        _records_for(root, download_attempts_file, limit),
        dedupe_keys=["execution_id", "service", "year", "month", "attempt_number"],
        time_columns=("started_at", "finished_at", "attempted_at", "written_at"),
        sort_column="attempted_at",
    )
    if not attempts.empty:
        attempts["attempt_kind"] = "download"
    inventory = _load_inventory(root, inventory_current_file)
    return JsonlAuditData(
        runs=runs,
        attempts=attempts,
        quality=quality,
        datasets=datasets,
        coverage=coverage,
        inventory=inventory,
    )


class JsonlAuditRepository:
    def __init__(self) -> None:
        dashboard = load_dashboard_config()
        raw = audit_config().get("filesystem", {})
        root_value = dashboard.get("paths", {}).get("audit_root") or raw.get("root", "data/audit")
        self.root = project_path(root_value)
        self._settings = (
            str(raw.get("pipeline_runs_file", "pipeline_runs.jsonl")),
            str(raw.get("dataset_events_file", "dataset_events.jsonl")),
            str(raw.get("quality_events_file", "quality_events.jsonl")),
            str(raw.get("coverage_snapshots_file", "coverage_snapshots.jsonl")),
            str(raw.get("download_attempts_file", "download_attempts.jsonl")),
            str(raw.get("inventory_current_file", "medallion_inventory.json")),
        )
        self._limit = int(audit_config().get("retention", {}).get("max_dashboard_documents", 20000))

    def load(self) -> JsonlAuditData:
        files = list(self.root.rglob("*.jsonl")) if self.root.is_dir() else []
        current = self.root / "inventory" / self._settings[-1]
        all_files = files + ([current] if current.is_file() else [])
        signature = (
            len(all_files),
            max((path.stat().st_mtime_ns for path in all_files), default=0),
            sum((path.stat().st_size for path in all_files), 0),
        )
        return _load_jsonl_cached(
            str(self.root), signature, self._settings, self._limit
        )
