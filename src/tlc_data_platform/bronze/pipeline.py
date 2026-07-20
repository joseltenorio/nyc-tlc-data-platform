from __future__ import annotations

import logging
import inspect
import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests

from tlc_data_platform.audit.availability_repository import AvailabilityRepository
from tlc_data_platform.audit.execution_repository import ExecutionRepository
from tlc_data_platform.audit.file_registry_repository import FileRegistryRepository
from tlc_data_platform.audit.file_version_repository import FileVersionRepository
from tlc_data_platform.audit.summaries import AuditRepositories
from tlc_data_platform.audit.parquet_metrics import parquet_metrics
from tlc_data_platform.audit.unified import UnifiedAuditRepository
from tlc_data_platform.bronze.manifest import ManifestWriter
from tlc_data_platform.bronze.models import (
    AvailabilityRecord,
    DEFERRED_REMOTE_ACCESS,
    DownloadResult,
    ExecutionSummary,
    FileCandidate,
    FileOutcome,
    PlanSummary,
    RemoteMetadata,
    classify_remote_availability,
    utc_now,
)
from tlc_data_platform.bronze.storage import BronzeStorage
from tlc_data_platform.core.exceptions import DownloadError
from tlc_data_platform.core.settings import AppConfig, RunSelection
from tlc_data_platform.core.spark import SparkProvider
from tlc_data_platform.ingestion.discovery import FileDiscovery
from tlc_data_platform.ingestion.downloader import FileDownloader
from tlc_data_platform.ingestion.http_client import HttpClient
from tlc_data_platform.ingestion.parquet_validator import ParquetValidator
from tlc_data_platform.ingestion.remote_probe import RemoteProbe
from tlc_data_platform.mongodb.client import MongoClientProvider
from tlc_data_platform.mongodb.index_manager import MongoIndexManager

LOGGER = logging.getLogger(__name__)


class BronzePipeline:
    """Coordinates discovery, concurrent download, validation, publication and audit."""

    def __init__(
        self,
        config: AppConfig,
        *,
        http: HttpClient | None = None,
        probe: RemoteProbe | None = None,
        discovery: FileDiscovery | None = None,
        storage: BronzeStorage | None = None,
        downloader: FileDownloader | None = None,
        validator: ParquetValidator | None = None,
        spark: SparkProvider | None = None,
        mongo_provider: MongoClientProvider | None = None,
        audit: AuditRepositories | None = None,
    ) -> None:
        self._config = config
        self._http = http or HttpClient(config.discovery, config.download)
        self._probe = probe or RemoteProbe(self._http)
        self._storage = storage or BronzeStorage(config.storage)
        self._discovery = discovery or FileDiscovery(config, self._http, self._probe)
        self._downloader = downloader or FileDownloader(
            self._http, self._storage, config.download
        )
        self._validator = validator or ParquetValidator(
            config.schema_contracts, config.validation
        )
        self._spark = spark or SparkProvider(config.spark)
        self._mongo_provider = mongo_provider or MongoClientProvider(config.mongo)
        self._audit = audit

    def close(self) -> None:
        self._spark.close()
        self._http.close()
        self._mongo_provider.close()

    def _ensure_audit(self) -> AuditRepositories:
        if self._audit is None:
            database = self._mongo_provider.database()
            MongoIndexManager(database, self._config.mongo).ensure_indexes()
            names = self._config.mongo.collections
            executions = ExecutionRepository(database[names.pipeline_executions])
            self._audit = AuditRepositories(
                executions=executions,
                availability=AvailabilityRepository(database[names.file_availability]),
                registry=FileRegistryRepository(
                    database[names.file_registry],
                    self._config.download.claim_ttl_minutes,
                    execution_repository=executions,
                ),
                versions=FileVersionRepository(database[names.file_versions]),
                unified=UnifiedAuditRepository(database, self._config.audit),
            )
        return self._audit

    def plan(self, selection: RunSelection) -> PlanSummary:
        self._storage.ensure_directories()
        plan_id = f"plan-{utc_now().strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
        discovery = self._discovery.discover(
            plan_id,
            selection.services,
            selection.start_year,
            selection.end_year,
            selection.months,
        )
        remote_map = self._probe_candidates(discovery.candidates, selection.workers)
        availability = self._merge_probe_results(discovery.availability, remote_map)

        warnings: list[str] = []
        if discovery.html_error:
            warnings.append(
                "El HTML oficial no pudo consultarse; se usó el fallback determinista."
            )

        registry: FileRegistryRepository | None = None
        try:
            registry = self._ensure_audit().registry
        except Exception as exc:
            warnings.append(
                f"MongoDB no estuvo disponible para consultar archivos procesados: {exc}"
            )

        already_processed = 0
        estimated_bytes = 0
        unknown_size = 0
        candidate_details: list[dict[str, Any]] = []
        for candidate in discovery.candidates:
            remote = remote_map.get(candidate.period_id, RemoteMetadata(False))
            existing = registry.get(candidate) if registry is not None else None
            processed = self._is_unchanged(
                existing,
                self._storage.final_path(candidate),
                remote,
            )
            if processed:
                already_processed += 1
            if remote.available:
                if remote.content_length is None:
                    unknown_size += 1
                else:
                    estimated_bytes += remote.content_length
            candidate_details.append(
                {
                    **candidate.to_dict(),
                    "remote": remote.to_dict(),
                    "already_processed": processed,
                }
            )

        free_space = self._storage.free_space_bytes()
        if unknown_size:
            warnings.append(
                f"{unknown_size} archivo(s) no informaron Content-Length; la estimación es parcial."
            )
        if estimated_bytes + self._storage.minimum_free_space_bytes > free_space:
            warnings.append(
                "El tamaño remoto estimado más la reserva mínima supera el espacio libre."
            )
        failed_probes = sum(item.status == "FAILED_TO_PROBE" for item in availability)
        if failed_probes:
            warnings.append(f"{failed_probes} periodo(s) no pudieron verificarse por red.")
        deferred_periods = sum(
            item.status == DEFERRED_REMOTE_ACCESS for item in availability
        )
        if deferred_periods:
            warnings.append(
                f"{deferred_periods} periodo(s) quedaron diferidos por acceso remoto temporal."
            )

        available_files = sum(item.status == "AVAILABLE" for item in availability)
        pending = max(0, available_files - already_processed)
        return PlanSummary(
            plan_type="PLAN",
            services=selection.services,
            start_year=selection.start_year,
            end_year=selection.end_year,
            months=selection.months,
            expected_periods=len(discovery.expected_periods),
            applicable_periods=sum(p.applicable for p in discovery.expected_periods),
            not_applicable_periods=sum(not p.applicable for p in discovery.expected_periods),
            available_files=available_files,
            not_published_files=sum(
                item.status == "NOT_PUBLISHED_YET" for item in availability
            ),
            failed_probes=failed_probes,
            already_processed_files=already_processed,
            pending_files=pending,
            estimated_remote_bytes=estimated_bytes,
            unknown_size_files=unknown_size,
            free_space_bytes=free_space,
            minimum_free_space_bytes=self._storage.minimum_free_space_bytes,
            workers=selection.workers,
            max_hvfhv_workers=selection.max_hvfhv_workers,
            warnings=warnings,
            candidates=candidate_details,
        )

    def run(
        self,
        selection: RunSelection,
        *,
        execution_type: str,
        dry_run: bool = False,
        force: bool = False,
    ) -> ExecutionSummary:
        started_at = utc_now()
        execution_id = (
            f"run-{started_at.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
        )
        self._storage.ensure_directories()
        audit = self._ensure_audit()
        self._cleanup_orphan_temporary_files(audit)
        manifest = ManifestWriter(self._config.storage.manifests_root, execution_id)
        audit.executions.start(
            execution_id,
            "DRY_RUN" if dry_run else execution_type.upper(),
            started_at,
            {
                "services": selection.services,
                "start_year": selection.start_year,
                "end_year": selection.end_year,
                "months": selection.months,
                "workers": selection.workers,
                "max_hvfhv_workers": selection.max_hvfhv_workers,
                "force": force,
                "dry_run": dry_run,
            },
        )
        if audit.unified is not None:
            audit.unified.start_run(
                execution_id,
                layer="bronze",
                execution_type="DRY_RUN" if dry_run else execution_type.upper(),
                selection={
                    "services": selection.services,
                    "start_year": selection.start_year,
                    "end_year": selection.end_year,
                    "months": selection.months,
                    "max_retries": self._config.download.max_retries,
                },
                started_at=started_at,
            )

        outcomes: list[FileOutcome] = []
        availability: list[AvailabilityRecord] = []
        discovery = None
        try:
            discovery = self._discovery.discover(
                execution_id,
                selection.services,
                selection.start_year,
                selection.end_year,
                selection.months,
            )
            remote_map = self._probe_candidates(
                discovery.candidates, selection.workers
            )
            availability = self._merge_probe_results(
                discovery.availability, remote_map
            )
            audit.availability.insert_many(availability)
            manifest.set_availability(availability)

            available_candidates = [
                candidate
                for candidate in discovery.candidates
                if remote_map.get(candidate.period_id, RemoteMetadata(False)).available
            ]

            if dry_run:
                for candidate in available_candidates:
                    outcome = FileOutcome(candidate=candidate, status="DRY_RUN")
                    outcome.remote_metadata = remote_map[candidate.period_id]
                    outcome.finish()
                    outcomes.append(outcome)
                    manifest.add_outcome(outcome)
                return self._finish_execution(
                    audit,
                    manifest,
                    execution_id,
                    "DRY_RUN",
                    started_at,
                    selection,
                    discovery,
                    availability,
                    outcomes,
                )

            pending: list[tuple[FileCandidate, RemoteMetadata, dict[str, Any] | None]] = []
            for candidate in available_candidates:
                remote = remote_map[candidate.period_id]
                existing = audit.registry.get(candidate)
                destination = self._storage.final_path(candidate)
                if not force and self._is_unchanged(existing, destination, remote):
                    outcome = FileOutcome(
                        candidate=candidate,
                        status="SKIPPED_UNCHANGED",
                        remote_metadata=remote,
                        local_path=str(destination),
                    )
                    current = (existing or {}).get("current") or {}
                    outcome.sha256 = current.get("sha256")
                    outcome.finish()
                    outcomes.append(outcome)
                    manifest.add_outcome(outcome)
                    continue

                if not audit.registry.claim(candidate, execution_id):
                    outcome = FileOutcome(
                        candidate=candidate,
                        status="SKIPPED_CLAIMED",
                        remote_metadata=remote,
                    )
                    outcome.finish()
                    outcomes.append(outcome)
                    manifest.add_outcome(outcome)
                    continue
                pending.append((candidate, remote, existing))

            downloaded, failed_downloads = self._download_pending(
                pending,
                execution_id,
                selection,
                audit,
            )
            for outcome in failed_downloads:
                outcomes.append(outcome)
                manifest.add_outcome(outcome)

            if downloaded:
                spark = self._spark.get()
                spark_guard = (
                    self._spark.guard() if hasattr(self._spark, "guard") else None
                )
                for result, existing in downloaded:
                    outcome = self._validate_and_publish(
                        result,
                        existing,
                        execution_id,
                        spark,
                        audit,
                        spark_guard=spark_guard,
                    )
                    outcomes.append(outcome)
                    manifest.add_outcome(outcome)
                    if (
                        outcome.status == "FAILED"
                        and not selection.continue_on_error
                    ):
                        break

            return self._finish_execution(
                audit,
                manifest,
                execution_id,
                execution_type.upper(),
                started_at,
                selection,
                discovery,
                availability,
                outcomes,
            )
        except Exception as exc:
            finished_at = utc_now()
            manifest_path: str | None = None
            if discovery is not None:
                failed_summary = self._build_summary(
                    execution_id,
                    execution_type.upper(),
                    "FAILED",
                    started_at,
                    finished_at,
                    selection,
                    discovery,
                    availability,
                    outcomes,
                    str(manifest.path),
                )
                try:
                    manifest_path = str(manifest.write(failed_summary))
                except OSError:
                    LOGGER.exception("No se pudo escribir el manifiesto de error")
            self._cleanup_execution_state(audit, execution_id)
            audit.executions.fail(
                execution_id, finished_at, exc, manifest_path
            )
            if audit.unified is not None:
                audit.unified.fail_run(execution_id, exc, layer="bronze")
            raise

    def _probe_candidates(
        self,
        candidates: list[FileCandidate],
        workers: int,
    ) -> dict[str, RemoteMetadata]:
        if not candidates:
            return {}
        result: dict[str, RemoteMetadata] = {
            candidate.period_id: RemoteMetadata(True)
            for candidate in candidates
            if candidate.discovery_method == "html"
        }
        probe_candidates = [
            candidate
            for candidate in candidates
            if candidate.discovery_method != "html"
        ]
        if not probe_candidates:
            return result
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self._probe.probe, candidate.url): candidate
                for candidate in probe_candidates
            }
            for future in as_completed(futures):
                candidate = futures[future]
                try:
                    result[candidate.period_id] = future.result()
                except Exception as exc:
                    result[candidate.period_id] = RemoteMetadata(
                        available=False,
                        probe_failed=True,
                        error_message=str(exc),
                    )
        return result

    @staticmethod
    def _merge_probe_results(
        availability: list[AvailabilityRecord],
        remote_map: dict[str, RemoteMetadata],
    ) -> list[AvailabilityRecord]:
        merged: list[AvailabilityRecord] = []
        for item in availability:
            remote = remote_map.get(item.period_id)
            if remote is None or item.status == "NOT_APPLICABLE":
                merged.append(item)
                continue
            if item.discovery_method != "html":
                item.status = classify_remote_availability(remote)
            item.remote_metadata = remote
            merged.append(item)
        return merged

    def _download_pending(
        self,
        pending: list[tuple[FileCandidate, RemoteMetadata, dict[str, Any] | None]],
        execution_id: str,
        selection: RunSelection,
        audit: AuditRepositories,
    ) -> tuple[
        list[tuple[DownloadResult, dict[str, Any] | None]],
        list[FileOutcome],
    ]:
        downloaded: list[tuple[DownloadResult, dict[str, Any] | None]] = []
        failures: list[FileOutcome] = []
        hvfhv_semaphore = threading.Semaphore(selection.max_hvfhv_workers)

        def task(
            candidate: FileCandidate,
            remote: RemoteMetadata,
            existing: dict[str, Any] | None,
        ) -> tuple[DownloadResult, dict[str, Any] | None]:
            semaphore = hvfhv_semaphore if candidate.service == "fhvhv" else None
            if semaphore is not None:
                semaphore.acquire()
            try:
                audit.registry.set_status(
                    candidate,
                    execution_id,
                    "DOWNLOADING",
                    remote_metadata=remote.to_dict(),
                )

                def on_attempt(event: dict[str, Any]) -> None:
                    if audit.unified is not None:
                        audit.unified.record_download_attempt(
                            execution_id,
                            service=candidate.service,
                            year=candidate.year,
                            month=candidate.month,
                            url=candidate.url,
                            **event,
                        )

                download_parameters = inspect.signature(
                    self._downloader.download
                ).parameters
                if "attempt_callback" in download_parameters:
                    result = self._downloader.download(
                        candidate, execution_id, remote, attempt_callback=on_attempt
                    )
                else:
                    # Keeps injected/test downloaders compatible while the real
                    # downloader records every retry through the callback.
                    result = self._downloader.download(
                        candidate, execution_id, remote
                    )
                audit.registry.set_status(
                    candidate,
                    execution_id,
                    "DOWNLOADED",
                    bytes_downloaded=result.bytes_downloaded,
                    sha256=result.sha256,
                )
                return result, existing
            finally:
                if semaphore is not None:
                    semaphore.release()

        max_workers = selection.workers if self._config.download.parallel_enabled else 1
        if max_workers == 1:
            consecutive_deferred = 0
            for index, (candidate, remote, existing) in enumerate(pending):
                try:
                    downloaded.append(task(candidate, remote, existing))
                    consecutive_deferred = 0
                except Exception as exc:
                    outcome = self._failure_outcome(candidate, exc)
                    self._storage.discard_temporary(candidate, execution_id)
                    if outcome.status == DEFERRED_REMOTE_ACCESS:
                        audit.registry.mark_deferred(outcome, execution_id)
                        consecutive_deferred += 1
                    else:
                        audit.registry.mark_failed(outcome, execution_id)
                        consecutive_deferred = 0
                    failures.append(outcome)
                    if consecutive_deferred >= 2:
                        for deferred_candidate, deferred_remote, _ in pending[index + 1 :]:
                            deferred = FileOutcome(
                                candidate=deferred_candidate,
                                status=DEFERRED_REMOTE_ACCESS,
                                remote_metadata=deferred_remote,
                            )
                            deferred.error_type = "RemoteAccessDeferred"
                            deferred.error_message = (
                                "Se abrió un corte preventivo tras bloqueos remotos consecutivos."
                            )
                            deferred.finish()
                            audit.registry.mark_deferred(deferred, execution_id)
                            failures.append(deferred)
                        break
            return downloaded, failures

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map: dict[
                Future[tuple[DownloadResult, dict[str, Any] | None]],
                FileCandidate,
            ] = {}
            for candidate, remote, existing in pending:
                future_map[executor.submit(task, candidate, remote, existing)] = candidate

            for future in as_completed(future_map):
                candidate = future_map[future]
                try:
                    downloaded.append(future.result())
                except Exception as exc:
                    outcome = self._failure_outcome(candidate, exc)
                    self._storage.discard_temporary(candidate, execution_id)
                    if outcome.status == DEFERRED_REMOTE_ACCESS:
                        audit.registry.mark_deferred(outcome, execution_id)
                    else:
                        audit.registry.mark_failed(outcome, execution_id)
                    failures.append(outcome)
        return downloaded, failures

    def _validate_and_publish(
        self,
        result: DownloadResult,
        existing: dict[str, Any] | None,
        execution_id: str,
        spark: Any,
        audit: AuditRepositories,
        *,
        spark_guard: Any | None = None,
    ) -> FileOutcome:
        candidate = result.candidate
        outcome = FileOutcome(
            candidate=candidate,
            status="VALIDATING",
            remote_metadata=result.remote_metadata,
            bytes_downloaded=result.bytes_downloaded,
            sha256=result.sha256,
            attempt_count=result.attempt_count,
            retry_count=result.retry_count,
            download_started_at=result.download_started_at,
            download_finished_at=result.download_finished_at,
            download_duration_seconds=result.download_duration_seconds,
            throughput_bytes_per_second=result.throughput_bytes_per_second,
        )
        current = (existing or {}).get("current") or {}
        previous_sha = current.get("sha256")
        destination = self._storage.final_path(candidate)

        try:
            audit.registry.set_status(candidate, execution_id, "VALIDATING")
            if previous_sha == result.sha256 and destination.is_file():
                result.path.unlink(missing_ok=True)
                outcome.status = "SKIPPED_UNCHANGED"
                outcome.local_path = str(destination)
                outcome.finish()
                audit.registry.release_claim(candidate, execution_id)
                return outcome

            def validation_action() -> Any:
                return self._validator.validate(result.path, candidate, spark)
            validation = (
                spark_guard.run(validation_action)
                if spark_guard is not None
                else validation_action()
            )
            final_path, archived_path = self._storage.promote(
                result.path,
                candidate,
                previous_sha256=previous_sha,
                new_sha256=result.sha256,
            )
            outcome.validation = validation
            outcome.local_path = str(final_path)
            outcome.archived_previous_path = (
                str(archived_path) if archived_path is not None else None
            )
            outcome.status = "READY"
            outcome.finish()

            if archived_path is not None:
                audit.versions.mark_archived(
                    candidate.service,
                    candidate.year,
                    candidate.month,
                    previous_sha,
                    str(archived_path),
                    execution_id,
                )
            audit.versions.insert_current(outcome, execution_id)
            audit.registry.mark_ready(outcome, execution_id)
            return outcome
        except Exception as exc:
            result.path.unlink(missing_ok=True)
            outcome.status = "FAILED"
            outcome.error_type = type(exc).__name__
            outcome.error_message = str(exc)
            outcome.finish()
            audit.registry.mark_failed(outcome, execution_id)
            return outcome

    def _finish_execution(
        self,
        audit: AuditRepositories,
        manifest: ManifestWriter,
        execution_id: str,
        execution_type: str,
        started_at: Any,
        selection: RunSelection,
        discovery: Any,
        availability: list[AvailabilityRecord],
        outcomes: list[FileOutcome],
    ) -> ExecutionSummary:
        finished_at = utc_now()
        failed = sum(outcome.status == "FAILED" for outcome in outcomes)
        deferred = sum(
            outcome.status == DEFERRED_REMOTE_ACCESS for outcome in outcomes
        ) + sum(item.status == DEFERRED_REMOTE_ACCESS for item in availability)
        failed_probes = sum(item.status == "FAILED_TO_PROBE" for item in availability)
        completed = sum(
            outcome.status in {"READY", "SKIPPED_UNCHANGED", "DRY_RUN"}
            for outcome in outcomes
        )
        claimed = sum(
            outcome.status == "SKIPPED_CLAIMED" for outcome in outcomes
        )
        available = sum(item.status == "AVAILABLE" for item in availability)
        incomplete = completed < available

        # SUCCESS means that every remotely available period has a usable result.
        # A claim is not evidence of an existing file: it only means that another
        # execution owns (or previously owned) the period. Treating claimed files
        # as successful allowed Silver to start with an incomplete Bronze layer.
        status = "SUCCESS"
        if failed or deferred or failed_probes or claimed or incomplete:
            status = "PARTIAL_SUCCESS" if completed else "FAILED"
        summary = self._build_summary(
            execution_id,
            execution_type,
            status,
            started_at,
            finished_at,
            selection,
            discovery,
            availability,
            outcomes,
            str(manifest.path),
        )
        manifest.write(summary)
        self._cleanup_execution_state(audit, execution_id)
        audit.executions.finish(summary)
        self._record_unified_audit(
            audit, execution_id, summary, availability, outcomes
        )
        return summary

    def _record_unified_audit(
        self,
        audit: AuditRepositories,
        execution_id: str,
        summary: ExecutionSummary,
        availability: list[AvailabilityRecord],
        outcomes: list[FileOutcome],
    ) -> None:
        unified = audit.unified
        if unified is None:
            return
        for outcome in outcomes:
            validation = outcome.validation
            physical = (
                parquet_metrics(Path(outcome.local_path))
                if outcome.local_path
                else None
            )
            unified.record_dataset(
                execution_id,
                layer="bronze",
                dataset_name=outcome.candidate.file_name,
                dataset_type="source_parquet",
                operation="download_validate_publish",
                status=outcome.status,
                path=outcome.local_path,
                parquet_files=physical.parquet_files if physical else None,
                rows=(
                    validation.parquet_num_rows
                    if validation is not None
                    else (physical.rows if physical else None)
                ),
                bytes_on_disk=(
                    physical.bytes_on_disk
                    if physical is not None
                    else outcome.bytes_downloaded
                ),
                service=outcome.candidate.service,
                year=outcome.candidate.year,
                month=outcome.candidate.month,
                metadata={
                    "sha256": outcome.sha256,
                    "attempt_count": outcome.attempt_count,
                    "retry_count": outcome.retry_count,
                    "schema_hash": validation.schema_hash if validation else None,
                    "download_started_at": outcome.download_started_at,
                    "download_finished_at": outcome.download_finished_at,
                    "download_duration_seconds": outcome.download_duration_seconds,
                    "throughput_bytes_per_second": outcome.throughput_bytes_per_second,
                },
            )
            if outcome.status == "READY" and validation is not None:
                required_ok = not validation.missing_required_columns and not validation.type_mismatches
                unified.record_quality(
                    execution_id,
                    layer="bronze",
                    dataset_name=outcome.candidate.file_name,
                    rule_code="BRONZE_SCHEMA_CONTRACT",
                    dimension="validity",
                    severity="ERROR",
                    status="PASSED" if required_ok else "FAILED",
                    expected="required columns and compatible types",
                    actual={
                        "missing_required": validation.missing_required_columns,
                        "type_mismatches": validation.type_mismatches,
                    },
                    failed_rows=0 if required_ok else validation.parquet_num_rows,
                    context={"service": outcome.candidate.service, "year": outcome.candidate.year, "month": outcome.candidate.month},
                )
                unified.record_quality(
                    execution_id,
                    layer="bronze",
                    dataset_name=outcome.candidate.file_name,
                    rule_code="BRONZE_NON_EMPTY_PARQUET",
                    dimension="completeness",
                    severity="ERROR",
                    status="PASSED" if validation.parquet_num_rows > 0 else "FAILED",
                    expected="> 0 rows",
                    actual=validation.parquet_num_rows,
                    failed_rows=0 if validation.parquet_num_rows > 0 else 1,
                )
            elif outcome.status in {"FAILED", DEFERRED_REMOTE_ACCESS}:
                unified.record_quality(
                    execution_id,
                    layer="bronze",
                    dataset_name=outcome.candidate.file_name,
                    rule_code="BRONZE_FILE_PROCESSING",
                    dimension="availability",
                    severity="ERROR" if outcome.status == "FAILED" else "WARNING",
                    status="FAILED" if outcome.status == "FAILED" else "WARNING",
                    message=outcome.error_message,
                    context={"error_type": outcome.error_type},
                )

        outcome_by_period = {
            outcome.candidate.period_id: outcome for outcome in outcomes
        }
        ready_periods = {
            period_id
            for period_id, outcome in outcome_by_period.items()
            if outcome.status in {"READY", "SKIPPED_UNCHANGED"}
        }
        missing_set: set[str] = set()
        details: list[dict[str, Any]] = []
        for item in availability:
            outcome = outcome_by_period.get(item.period_id)
            physical_status = outcome.status if outcome is not None else None
            detail = item.to_dict()
            detail["processing_status"] = physical_status
            detail["ready"] = item.period_id in ready_periods
            details.append(detail)
            if not item.applicable:
                continue
            if (
                item.status == "NOT_PUBLISHED_YET"
                and not self._config.audit.treat_not_published_as_missing
            ):
                continue
            if item.status != "AVAILABLE" or item.period_id not in ready_periods:
                missing_set.add(item.period_id)
        missing = sorted(missing_set)
        unified.record_coverage(
            execution_id,
            layer="bronze",
            expected_count=summary.expected_periods,
            available_count=summary.available_files,
            ready_count=len(ready_periods),
            missing=missing,
            not_applicable_count=summary.not_applicable_periods,
            not_published_count=summary.not_published_files,
            deferred_count=sum(item.status == DEFERRED_REMOTE_ACCESS for item in availability),
            details=details,
        )
        expected_published = max(
            0,
            summary.expected_periods
            - summary.not_applicable_periods
            - (
                summary.not_published_files
                if not self._config.audit.treat_not_published_as_missing
                else 0
            ),
        )
        unified.record_quality(
            execution_id,
            layer="bronze",
            dataset_name="bronze_layer",
            rule_code="BRONZE_EXPECTED_DATASETS_READY",
            dimension="completeness",
            severity="ERROR",
            status="PASSED" if not missing else "FAILED",
            expected=expected_published,
            actual=len(ready_periods),
            failed_rows=len(missing),
            context={"missing_periods": missing},
        )
        unified.finish_run(
            execution_id,
            status=summary.status,
            finished_at=summary.finished_at,
            metrics={
                "parquet_files_expected": summary.applicable_periods,
                "parquet_files_available": summary.available_files,
                "parquet_files_processed": summary.ready_files,
                "parquet_files_skipped": summary.skipped_files,
                "parquet_files_failed": summary.failed_files,
                "download_attempts": summary.total_download_attempts,
                "download_retries": summary.total_retries,
                "bytes_downloaded": summary.total_bytes_downloaded,
                "download_seconds": summary.total_download_seconds,
                "average_download_mbps": summary.average_download_mbps,
                "error_rate": summary.error_rate,
            },
        )

    @staticmethod
    def _build_summary(
        execution_id: str,
        execution_type: str,
        status: str,
        started_at: Any,
        finished_at: Any,
        selection: RunSelection,
        discovery: Any,
        availability: list[AvailabilityRecord],
        outcomes: list[FileOutcome],
        manifest_path: str,
    ) -> ExecutionSummary:
        attempted_outcomes = [
            outcome for outcome in outcomes if outcome.attempt_count > 0
        ]
        recorded_durations = [
            outcome.download_duration_seconds for outcome in attempted_outcomes
        ]
        total_download_seconds = (
            sum(float(value) for value in recorded_durations if value is not None)
            if attempted_outcomes and all(value is not None for value in recorded_durations)
            else None
        )
        total_bytes_downloaded = sum(outcome.bytes_downloaded or 0 for outcome in outcomes)
        attempted_files = sum(outcome.status in {"READY", "FAILED"} for outcome in outcomes)
        failed_files = sum(outcome.status == "FAILED" for outcome in outcomes)
        average_download_mbps = (
            (total_bytes_downloaded * 8) / total_download_seconds / 1_000_000
            if total_download_seconds is not None and total_download_seconds > 0
            else None
        )
        return ExecutionSummary(
            execution_id=execution_id,
            execution_type=execution_type,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            requested_services=selection.services,
            requested_start_year=selection.start_year,
            requested_end_year=selection.end_year,
            requested_months=selection.months,
            expected_periods=len(discovery.expected_periods),
            applicable_periods=sum(p.applicable for p in discovery.expected_periods),
            available_files=sum(item.status == "AVAILABLE" for item in availability),
            downloaded_files=sum(
                outcome.bytes_downloaded is not None
                and outcome.status in {"READY", "FAILED", "SKIPPED_UNCHANGED"}
                for outcome in outcomes
            ),
            ready_files=sum(outcome.status == "READY" for outcome in outcomes),
            skipped_files=sum(
                outcome.status in {"SKIPPED_UNCHANGED", "SKIPPED_CLAIMED"}
                for outcome in outcomes
            ),
            failed_files=failed_files,
            failed_probe_periods=sum(
                item.status == "FAILED_TO_PROBE" for item in availability
            ),
            not_published_files=sum(
                item.status == "NOT_PUBLISHED_YET" for item in availability
            ),
            not_applicable_periods=sum(
                item.status == "NOT_APPLICABLE" for item in availability
            ),
            total_bytes_downloaded=total_bytes_downloaded,
            manifest_path=manifest_path,
            total_download_attempts=sum(outcome.attempt_count for outcome in outcomes),
            total_retries=sum(outcome.retry_count for outcome in outcomes),
            total_download_seconds=total_download_seconds,
            average_download_mbps=average_download_mbps,
            error_rate=(failed_files / attempted_files if attempted_files else None),
        )

    @staticmethod
    def _failure_outcome(candidate: FileCandidate, exc: Exception) -> FileOutcome:
        outcome = FileOutcome(
            candidate=candidate,
            status=(
                DEFERRED_REMOTE_ACCESS
                if BronzePipeline._is_temporary_remote_failure(exc)
                else "FAILED"
            ),
        )
        outcome.error_type = type(exc).__name__
        outcome.error_message = str(exc)
        outcome.attempt_count = int(getattr(exc, "attempt_count", 0) or 0)
        outcome.retry_count = int(getattr(exc, "retry_count", 0) or 0)
        outcome.download_started_at = getattr(exc, "download_started_at", None)
        outcome.download_finished_at = getattr(exc, "download_finished_at", None)
        outcome.download_duration_seconds = getattr(exc, "download_duration_seconds", None)
        if isinstance(exc, requests.HTTPError) and exc.response is not None:
            outcome.remote_metadata = RemoteMetadata(
                available=False,
                status_code=exc.response.status_code,
                content_length=None,
                etag=exc.response.headers.get("ETag"),
                last_modified=exc.response.headers.get("Last-Modified"),
                content_type=exc.response.headers.get("Content-Type"),
            )
        outcome.finish()
        return outcome

    @staticmethod
    def _is_temporary_remote_failure(exc: Exception) -> bool:
        if isinstance(exc, requests.HTTPError) and exc.response is not None:
            return exc.response.status_code in {202, 403, 429, 500, 502, 503, 504}
        if isinstance(exc, DownloadError):
            message = str(exc)
            return any(
                marker in message
                for marker in (
                    "HTML",
                    "Firma Parquet inválida",
                    "Archivo incompleto",
                )
            )
        return False

    def _cleanup_orphan_temporary_files(self, audit: AuditRepositories) -> None:
        removed = 0
        for path, owner_execution_id in self._storage.temporary_entries():
            if audit.executions.is_active(owner_execution_id):
                continue
            LOGGER.info(
                "Eliminando temporal huérfano %s de la ejecución %s",
                path,
                owner_execution_id,
            )
            self._storage.discard_temporary_path(path)
            removed += 1
        if removed:
            LOGGER.info("Se eliminaron %s temporales huérfanos", removed)

    def _cleanup_execution_state(
        self,
        audit: AuditRepositories,
        execution_id: str,
    ) -> None:
        released = audit.registry.release_claims_for_execution(execution_id)
        removed = self._storage.discard_temporary_for_execution(execution_id)
        if released:
            LOGGER.info(
                "Se liberaron %s claim(s) pendientes de la ejecución %s",
                released,
                execution_id,
            )
        if removed:
            LOGGER.info(
                "Se eliminaron %s temporal(es) residuales de la ejecución %s",
                removed,
                execution_id,
            )

    @staticmethod
    def _is_unchanged(
        existing: dict[str, Any] | None,
        destination: Path,
        remote: RemoteMetadata,
    ) -> bool:
        if not existing or not destination.is_file():
            return False
        if destination.stat().st_size <= 0:
            return False
        current = existing.get("current") or {}
        if current.get("status") != "READY":
            return False
        local_path = current.get("local_path")
        if local_path is not None and Path(local_path).parts[-4:] != destination.parts[-4:]:
            return False
        previous_remote = current.get("remote_metadata") or {}
        compared = 0
        for field in ("etag", "last_modified", "content_length"):
            old = previous_remote.get(field)
            new = getattr(remote, field)
            if old is not None and new is not None:
                compared += 1
                if old != new:
                    return False
        if compared:
            return True
        expected_size = current.get("bytes_downloaded")
        return expected_size is None or destination.stat().st_size == expected_size