from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd
import streamlit as st

from dashboard.config import audit_config, gold_config, load_dashboard_config, ml_config
from dashboard.data_access.jsonl_audit_repository import JsonlAuditData, JsonlAuditRepository
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
    inventory: pd.DataFrame
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
    return list(collection.find({}, {"_id": 0}).sort(sort_field, -1).limit(limit))


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
        limit = int(audit_config().get("retention", {}).get("max_dashboard_documents", 20000))
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
                _mongo_documents(database[name], sort_field=sort_field, limit=limit)
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


def _present(value: Any) -> bool:
    if value is None:
        return False
    try:
        return not bool(pd.isna(value))
    except (TypeError, ValueError):
        return True


def _freshness_value(row: dict[str, Any], preferred: str | None = None) -> int:
    candidates = [
        "updated_at",
        "written_at",
        "finished_at",
        preferred,
        "checked_at",
        "recorded_at",
        "attempted_at",
        "started_at",
        "occurred_at",
    ]
    values: list[int] = []
    for column in candidates:
        if not column or column not in row or not _present(row.get(column)):
            continue
        parsed = pd.to_datetime(row.get(column), utc=True, errors="coerce")
        if not pd.isna(parsed):
            values.append(int(parsed.value))
    return max(values, default=-(2**63))


def _coalesce_frames(
    frames: Iterable[pd.DataFrame],
    *,
    keys: list[str],
    sort_column: str | None = None,
) -> pd.DataFrame:
    valid_frames = [
        frame.copy()
        for frame in frames
        if frame is not None and not frame.empty
    ]

    if not valid_frames:
        return pd.DataFrame()

    result = pd.concat(
        valid_frames,
        ignore_index=True,
        sort=False,
    )

    datetime_columns = (
        "started_at",
        "finished_at",
        "updated_at",
        "written_at",
        "checked_at",
        "recorded_at",
        "attempted_at",
        "occurred_at",
        "created_at",
        "captured_at",
        "latest_modified_at",
    )

    for column in datetime_columns:
        if column in result.columns:
            result[column] = pd.to_datetime(
                result[column],
                utc=True,
                errors="coerce",
            )

    existing_keys = [
        key
        for key in keys
        if key in result.columns
    ]

    if existing_keys:
        if sort_column and sort_column in result.columns:
            result = result.sort_values(
                sort_column,
                ascending=True,
                na_position="first",
            )

        result = result.drop_duplicates(
            subset=existing_keys,
            keep="last",
        )

    if sort_column and sort_column in result.columns:
        result = result.sort_values(
            sort_column,
            ascending=False,
            na_position="last",
        )

    return result.reset_index(drop=True)


def _concat_nonempty(frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    selected = [frame for frame in frames if not frame.empty]
    return pd.concat(selected, ignore_index=True, sort=False) if selected else pd.DataFrame()


def _normalize_legacy_quality(frame: pd.DataFrame, layer: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    if "layer" not in result.columns:
        result["layer"] = layer
    else:
        result["layer"] = result["layer"].fillna(layer)
    if "checked_at" not in result.columns and "recorded_at" in result.columns:
        result["checked_at"] = result["recorded_at"]
    if "failed_rows" not in result.columns:
        if "affected_rows" in result.columns:
            result["failed_rows"] = pd.to_numeric(
                result["affected_rows"], errors="coerce"
            )
        else:
            result["failed_rows"] = pd.Series(
                pd.NA, index=result.index, dtype="Float64"
            )
    if "dimension" not in result.columns:
        result["dimension"] = "validity"
    if "dataset_name" not in result.columns:
        if "service" in result.columns:
            result["dataset_name"] = result["service"].astype(str) + "_trips"
        else:
            result["dataset_name"] = "legacy_quality"
    if "status" not in result.columns:
        affected = pd.to_numeric(
            result.get(
                "affected_rows",
                pd.Series(pd.NA, index=result.index, dtype="Float64"),
            ),
            errors="coerce",
        )
        severity = result.get(
            "severity", pd.Series("UNKNOWN", index=result.index)
        ).astype(str).str.upper()
        result["status"] = "UNKNOWN"
        result.loc[affected.eq(0), "status"] = "PASSED"
        result.loc[affected.gt(0) & severity.eq("ERROR"), "status"] = "FAILED"
        result.loc[affected.gt(0) & ~severity.eq("ERROR"), "status"] = "WARNING"
    result["source_store"] = "mongodb_legacy"
    return result


def _quality_key(frame: pd.DataFrame) -> pd.Series:
    def values(*names: str) -> pd.Series:
        result = pd.Series("", index=frame.index, dtype="object")
        for name in names:
            if name not in frame.columns:
                continue
            candidate = frame[name].fillna("").astype(str)
            result = result.mask(result.eq(""), candidate)
        return result

    parts = [
        values("execution_id"),
        values("layer"),
        values("dataset_name"),
        values("rule_code"),
        values("service", "context.service"),
        values("year", "context.year"),
        values("month", "context.month"),
    ]
    key = parts[0]
    for part in parts[1:]:
        key = key + "|" + part
    return key


def _merge_quality(
    mongo_unified: pd.DataFrame,
    jsonl_quality: pd.DataFrame,
    silver_legacy: pd.DataFrame,
    gold_legacy: pd.DataFrame,
) -> pd.DataFrame:
    unified = _coalesce_frames(
        [mongo_unified, jsonl_quality],
        keys=["quality_id"],
        sort_column="checked_at",
    )
    legacy = _concat_nonempty(
        [
            _normalize_legacy_quality(silver_legacy, "silver"),
            _normalize_legacy_quality(gold_legacy, "gold"),
        ]
    )
    combined = _concat_nonempty([unified, legacy])
    if combined.empty:
        return combined
    combined["_quality_key"] = _quality_key(combined)
    combined = _coalesce_frames(
        [combined], keys=["_quality_key"], sort_column="checked_at"
    )
    return combined.drop(columns=["_quality_key"], errors="ignore")


def _failed_documents(frame: pd.DataFrame, source: str) -> pd.DataFrame:
    if frame.empty or "status" not in frame.columns:
        return pd.DataFrame()
    failed = frame[
        frame["status"].astype(str).str.upper().isin(["FAILED", "ERROR", "EXHAUSTED"])
    ].copy()
    if failed.empty:
        return failed
    failed["source"] = source
    return failed


def _dedupe_errors(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    identity_columns = [
        column
        for column in (
            "execution_id",
            "layer",
            "service",
            "year",
            "month",
            "dataset_name",
            "rule_code",
            "attempt_number",
            "error_type",
            "error_message",
            "subject",
        )
        if column in result.columns
    ]
    if not identity_columns:
        return result.reset_index(drop=True)
    normalized = result[identity_columns].copy()
    for column in identity_columns:
        normalized[column] = normalized[column].map(
            lambda value: "" if not _present(value) else str(value)
        )
    result["_error_identity"] = normalized.agg("|".join, axis=1)
    time_column = next(
        (
            column
            for column in (
                "occurred_at",
                "attempted_at",
                "recorded_at",
                "checked_at",
                "finished_at",
            )
            if column in result.columns
        ),
        None,
    )
    if time_column:
        result[time_column] = pd.to_datetime(
            result[time_column], utc=True, errors="coerce"
        )
        result = result.sort_values(time_column, ascending=False, na_position="last")
    return (
        result.drop_duplicates("_error_identity", keep="first")
        .drop(columns=["_error_identity"], errors="ignore")
        .reset_index(drop=True)
    )


class AuditRepository:
    """Loads real audit facts from MongoDB, JSONL and legacy run manifests."""

    def load(self) -> AuditData:
        manifests: ManifestAuditData = ManifestAuditRepository().load()
        jsonl: JsonlAuditData = JsonlAuditRepository().load()
        config = load_dashboard_config().get("mongo", {})
        env_name = str(config.get("uri_environment_variable", "MONGODB_URI"))
        uri = os.getenv(env_name, str(config.get("default_uri", "mongodb://localhost:27017")))
        database = str(config.get("database", "nyc_tlc_audit"))
        timeout_ms = int(config.get("connect_timeout_ms", 1500))

        mongo_available = False
        mongo: dict[str, pd.DataFrame] = {}
        try:
            mongo = _load_mongo(uri, database, timeout_ms)
            mongo_available = True
        except Exception:
            mongo = {}

        mongo_runs = _normalize_layer_runs(mongo) if mongo_available else pd.DataFrame()
        runs = _coalesce_frames(
            [mongo_runs, jsonl.runs, manifests.runs],
            keys=["execution_id"],
            sort_column="started_at",
        )

        processing = mongo.get("processing_attempts", pd.DataFrame()).copy()
        if not processing.empty:
            processing["attempt_kind"] = "processing"
        mongo_downloads = mongo.get("download_attempts", pd.DataFrame()).copy()
        if not mongo_downloads.empty:
            mongo_downloads["attempt_kind"] = "download"
            if "attempted_at" in mongo_downloads.columns and "started_at" not in mongo_downloads.columns:
                mongo_downloads["started_at"] = mongo_downloads["attempted_at"]
        downloads = _coalesce_frames(
            [mongo_downloads, jsonl.attempts],
            keys=["execution_id", "service", "year", "month", "attempt_number"],
            sort_column="attempted_at",
        )
        attempts = _concat_nonempty([processing, downloads])

        quality = _merge_quality(
            mongo.get("unified_quality", pd.DataFrame()),
            jsonl.quality,
            mongo.get("silver_quality", pd.DataFrame()),
            mongo.get("gold_quality", pd.DataFrame()),
        )
        reconciliations = _concat_nonempty(
            [
                mongo.get("silver_reconciliations", pd.DataFrame()),
                mongo.get("gold_reconciliations", pd.DataFrame()),
            ]
        )
        datasets = _coalesce_frames(
            [mongo.get("datasets", pd.DataFrame()), jsonl.datasets],
            keys=["event_id"],
            sort_column="recorded_at",
        )
        coverage = _coalesce_frames(
            [mongo.get("coverage", pd.DataFrame()), jsonl.coverage],
            keys=["execution_id", "layer"],
            sort_column="checked_at",
        )

        errors = _dedupe_errors(
            _concat_nonempty(
                [
                    manifests.errors,
                    _failed_documents(runs, "pipeline_run"),
                    _failed_documents(attempts, "attempt"),
                    _failed_documents(datasets, "dataset_event"),
                    _failed_documents(quality, "quality_event"),
                ]
            )
        )
        active_sources: list[str] = []
        if mongo_available:
            active_sources.append("MongoDB")
        if any(
            not frame.empty
            for frame in (
                jsonl.runs,
                jsonl.attempts,
                jsonl.quality,
                jsonl.datasets,
                jsonl.coverage,
                jsonl.inventory,
            )
        ):
            active_sources.append("auditoría JSONL")
        if not manifests.runs.empty or not manifests.errors.empty:
            active_sources.append("manifests de compatibilidad")
        source_note = " + ".join(active_sources) or "sin fuentes de auditoría disponibles"
        if not mongo_available:
            source_note += " (MongoDB no disponible)"
        return AuditData(
            runs=runs,
            errors=errors,
            attempts=attempts,
            quality=quality,
            reconciliations=reconciliations,
            datasets=datasets,
            coverage=coverage,
            inventory=jsonl.inventory,
            source_note=source_note,
        )
