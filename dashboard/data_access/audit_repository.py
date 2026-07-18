from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import pandas as pd
import streamlit as st

from dashboard.config import audit_config, gold_config, load_dashboard_config, ml_config
from dashboard.data_access.manifest_repository import ManifestAuditData, ManifestAuditRepository


@dataclass(frozen=True)
class AuditData:
    runs: pd.DataFrame
    errors: pd.DataFrame
    attempts: pd.DataFrame
    quality: pd.DataFrame
    reconciliations: pd.DataFrame
    datasets: pd.DataFrame
    coverage: pd.DataFrame
    source_note: str


def _to_frame(documents: list[dict[str, Any]]) -> pd.DataFrame:
    if not documents:
        return pd.DataFrame()
    clean: list[dict[str, Any]] = []
    for document in documents:
        item = dict(document)
        item.pop("_id", None)
        clean.append(item)
    return pd.json_normalize(clean, sep=".")


def _mongo_documents(
    collection: Any,
    *,
    sort_field: str,
    limit: int = 20000,
) -> list[dict[str, Any]]:
    return list(
        collection.find({}, {"_id": 0}).sort(sort_field, -1).limit(limit)
    )


@st.cache_data(ttl=60, show_spinner=False)
def _load_mongo(uri: str, database_name: str, timeout_ms: int) -> dict[str, pd.DataFrame]:
    from pymongo import MongoClient

    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=timeout_ms,
        connectTimeoutMS=timeout_ms,
    )
    try:
        client.admin.command("ping")
        database = client[database_name]
        gold = gold_config().get("mongo_collections", {})
        ml = ml_config().get("mongo_collections", {})
        unified = audit_config().get("collections", {})
        limit = int(
            audit_config().get("retention", {}).get("max_dashboard_documents", 20000)
        )
        definitions = {
            "bronze_runs": ("pipeline_executions", "started_at"),
            "silver_runs": ("silver_pipeline_executions", "started_at"),
            "gold_runs": (
                gold.get("pipeline_executions", "gold_pipeline_executions"),
                "started_at",
            ),
            "ml_runs": (ml.get("training_runs", "ml_training_runs"), "started_at"),
            "unified_runs": (
                unified.get("pipeline_runs", "audit_pipeline_runs"),
                "started_at",
            ),
            "processing_attempts": (
                gold.get("processing_attempts", "processing_attempts"),
                "started_at",
            ),
            "download_attempts": (
                unified.get("download_attempts", "audit_download_attempts"),
                "attempted_at",
            ),
            "unified_quality": (
                unified.get("quality_events", "audit_quality_events"),
                "checked_at",
            ),
            "silver_quality": ("silver_quality_results", "checked_at"),
            "gold_quality": (
                gold.get("quality_results", "gold_quality_results"),
                "checked_at",
            ),
            "silver_reconciliations": ("silver_reconciliations", "checked_at"),
            "gold_reconciliations": (
                gold.get("reconciliations", "gold_reconciliations"),
                "checked_at",
            ),
            "datasets": (
                unified.get("dataset_events", "audit_dataset_events"),
                "recorded_at",
            ),
            "coverage": (
                unified.get("coverage_snapshots", "audit_coverage_snapshots"),
                "checked_at",
            ),
        }
        return {
            key: _to_frame(
                _mongo_documents(
                    database[name], sort_field=sort_field, limit=limit
                )
            )
            for key, (name, sort_field) in definitions.items()
        }
    finally:
        client.close()


def _normalize_layer_runs(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    layers = {
        "bronze_runs": "bronze",
        "silver_runs": "silver",
        "gold_runs": "gold",
        "ml_runs": "ml",
    }
    normalized: list[pd.DataFrame] = []
    unified = frames.get("unified_runs", pd.DataFrame()).copy()
    if not unified.empty:
        if "run_id" in unified.columns and "execution_id" not in unified.columns:
            unified = unified.rename(columns={"run_id": "execution_id"})
        normalized.append(unified)

    for key, layer in layers.items():
        frame = frames.get(key, pd.DataFrame()).copy()
        if frame.empty:
            continue
        if "run_id" in frame.columns and "execution_id" not in frame.columns:
            frame = frame.rename(columns={"run_id": "execution_id"})
        frame["layer"] = layer
        normalized.append(frame)

    if not normalized:
        return pd.DataFrame()
    result = pd.concat(normalized, ignore_index=True, sort=False)
    for column in ("started_at", "finished_at"):
        if column in result.columns:
            result[column] = pd.to_datetime(result[column], utc=True, errors="coerce")
    if "duration_seconds" not in result.columns:
        result["duration_seconds"] = pd.NA
    if {"started_at", "finished_at"}.issubset(result.columns):
        calculated = (result["finished_at"] - result["started_at"]).dt.total_seconds()
        result["duration_seconds"] = pd.to_numeric(
            result["duration_seconds"], errors="coerce"
        ).fillna(calculated)
    if "execution_id" in result.columns:
        result = result.sort_values(
            ["execution_id", "started_at"],
            ascending=[True, False],
            na_position="last",
        ).drop_duplicates("execution_id", keep="first")
    return result.sort_values("started_at", ascending=False, na_position="last")


def _merge_runs(manifest_runs: pd.DataFrame, mongo_runs: pd.DataFrame) -> pd.DataFrame:
    if manifest_runs.empty:
        return mongo_runs.reset_index(drop=True)
    if mongo_runs.empty:
        return manifest_runs.reset_index(drop=True)
    combined = pd.concat([mongo_runs, manifest_runs], ignore_index=True, sort=False)
    if "execution_id" in combined.columns:
        combined = combined.sort_values(
            ["execution_id", "started_at"],
            ascending=[True, False],
            na_position="last",
        ).drop_duplicates("execution_id", keep="first")
    return combined.sort_values(
        "started_at", ascending=False, na_position="last"
    ).reset_index(drop=True)


def _failed_documents(frame: pd.DataFrame, source: str) -> pd.DataFrame:
    if frame.empty or "status" not in frame.columns:
        return pd.DataFrame()
    failed = frame[
        frame["status"].astype(str).str.upper().isin(["FAILED", "ERROR"])
    ].copy()
    if failed.empty:
        return failed
    failed["source"] = source
    return failed


class AuditRepository:
    """Loads the unified audit contract and preserves manifest fallback."""

    def load(self) -> AuditData:
        manifests: ManifestAuditData = ManifestAuditRepository().load()
        config = load_dashboard_config().get("mongo", {})
        env_name = str(config.get("uri_environment_variable", "MONGODB_URI"))
        uri = os.getenv(
            env_name, str(config.get("default_uri", "mongodb://localhost:27017"))
        )
        database = str(config.get("database", "nyc_tlc_audit"))
        timeout_ms = int(config.get("connect_timeout_ms", 1500))
        try:
            mongo = _load_mongo(uri, database, timeout_ms)
            runs = _merge_runs(manifests.runs, _normalize_layer_runs(mongo))

            processing = mongo.get("processing_attempts", pd.DataFrame()).copy()
            if not processing.empty:
                processing["attempt_kind"] = "processing"
            downloads = mongo.get("download_attempts", pd.DataFrame()).copy()
            if not downloads.empty:
                downloads["attempt_kind"] = "download"
                if "attempted_at" in downloads.columns and "started_at" not in downloads.columns:
                    downloads["started_at"] = downloads["attempted_at"]
            attempts = pd.concat(
                [processing, downloads], ignore_index=True, sort=False
            )

            quality = pd.concat(
                [
                    mongo.get("unified_quality", pd.DataFrame()),
                    mongo.get("silver_quality", pd.DataFrame()),
                    mongo.get("gold_quality", pd.DataFrame()),
                ],
                ignore_index=True,
                sort=False,
            )
            reconciliations = pd.concat(
                [
                    mongo.get("silver_reconciliations", pd.DataFrame()),
                    mongo.get("gold_reconciliations", pd.DataFrame()),
                ],
                ignore_index=True,
                sort=False,
            )
            datasets = mongo.get("datasets", pd.DataFrame())
            coverage = mongo.get("coverage", pd.DataFrame())

            errors = pd.concat(
                [
                    manifests.errors,
                    _failed_documents(runs, "pipeline_run"),
                    _failed_documents(attempts, "mongodb_attempt"),
                    _failed_documents(datasets, "mongodb_dataset"),
                    _failed_documents(quality, "mongodb_quality"),
                ],
                ignore_index=True,
                sort=False,
            )
            source_note = "Auditoría unificada MongoDB + manifiestos JSON"
        except Exception:
            runs = manifests.runs
            errors = manifests.errors
            attempts = pd.DataFrame()
            quality = pd.DataFrame()
            reconciliations = pd.DataFrame()
            datasets = pd.DataFrame()
            coverage = pd.DataFrame()
            source_note = "Manifiestos JSON (MongoDB no disponible)"
        return AuditData(
            runs=runs,
            errors=errors,
            attempts=attempts,
            quality=quality,
            reconciliations=reconciliations,
            datasets=datasets,
            coverage=coverage,
            source_note=source_note,
        )
