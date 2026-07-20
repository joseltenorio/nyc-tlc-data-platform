from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import requests

from tlc_data_platform.bronze.models import DownloadResult, FileCandidate, RemoteMetadata
from tlc_data_platform.bronze.storage import BronzeStorage
from tlc_data_platform.core.exceptions import DownloadError, InsufficientDiskSpaceError
from tlc_data_platform.core.settings import DownloadConfig
from tlc_data_platform.ingestion.checksum import calculate_sha256
from tlc_data_platform.ingestion.http_client import HttpClient


def has_parquet_signature(path: Path) -> bool:
    if not path.is_file() or path.stat().st_size < 8:
        return False
    with path.open("rb") as handle:
        start = handle.read(4)
        handle.seek(-4, os.SEEK_END)
        end = handle.read(4)
    return start == b"PAR1" and end == b"PAR1"


def response_metadata(response: Any) -> RemoteMetadata:
    content_length = response.headers.get("Content-Length")
    return RemoteMetadata(
        available=response.status_code in {200, 206},
        status_code=response.status_code,
        content_length=int(content_length)
        if content_length and content_length.isdigit()
        else None,
        etag=response.headers.get("ETag"),
        last_modified=response.headers.get("Last-Modified"),
        content_type=response.headers.get("Content-Type"),
    )


class FileDownloader:
    """Downloads one complete Parquet with five bounded whole-file retries.

    Retries cover the HTTP request *and* streamed transfer. This is deliberately
    separate from discovery/probe retries: a broken stream, HTTP 202, 403, 429 or
    transient 5xx response must restart the complete `.part` file rather than
    leave a partial download that later fails validation.
    """

    DOWNLOAD_RETRYABLE_STATUS_CODES = HttpClient.RETRYABLE_STATUS_CODES | {202, 403}

    def __init__(
        self,
        http: HttpClient,
        storage: BronzeStorage,
        config: DownloadConfig,
    ) -> None:
        self._http = http
        self._storage = storage
        self._config = config

    def download(
        self,
        candidate: FileCandidate,
        execution_id: str,
        remote_metadata: RemoteMetadata,
        attempt_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> DownloadResult:
        temporary = self._storage.temporary_path(candidate, execution_id)
        temporary.parent.mkdir(parents=True, exist_ok=True)
        temporary.unlink(missing_ok=True)

        max_attempts = self._config.max_retries + 1
        backoff = self._config.initial_backoff_seconds
        last_error: Exception | None = None
        download_started_at = datetime.now(timezone.utc)
        download_started_perf = time.perf_counter()

        for attempt in range(1, max_attempts + 1):
            self._assert_disk_space(remote_metadata)
            response: Any | None = None
            temporary.unlink(missing_ok=True)
            attempt_started_at = datetime.now(timezone.utc)
            attempt_started_perf = time.perf_counter()
            try:
                # FileDownloader owns the retry loop. Disable HttpClient's inner
                # retries so one audit event equals one complete transfer attempt.
                response = self._http.request(
                    "GET",
                    candidate.url,
                    stream=True,
                    allow_redirects=True,
                    retryable_status_codes=set(),
                    max_retries_override=0,
                )
                status_code = int(response.status_code)
                if status_code in self.DOWNLOAD_RETRYABLE_STATUS_CODES:
                    error = requests.HTTPError(
                        f"HTTP temporal {status_code} durante la descarga",
                        response=response,
                    )
                    raise error
                response.raise_for_status()
                if status_code not in {200, 206}:
                    raise DownloadError(
                        f"Estado HTTP inesperado durante la descarga: {status_code}"
                    )

                content_type = (response.headers.get("Content-Type") or "").lower()
                effective_remote_metadata = response_metadata(response)
                if "text/html" in content_type:
                    raise DownloadError("El servidor devolvió HTML en lugar de Parquet")

                with temporary.open("wb") as handle:
                    for chunk in response.iter_content(self._config.chunk_size_bytes):
                        if chunk:
                            handle.write(chunk)
                    handle.flush()
                    os.fsync(handle.fileno())

                size = temporary.stat().st_size
                self._validate_download(
                    temporary,
                    size=size,
                    content_type=content_type,
                    expected_size=remote_metadata.content_length,
                )
                sha256 = (
                    calculate_sha256(temporary)
                    if self._config.calculate_sha256
                    else ""
                )
                attempt_finished_at = datetime.now(timezone.utc)
                attempt_duration = max(0.0, time.perf_counter() - attempt_started_perf)
                attempt_throughput = size / attempt_duration if attempt_duration > 0 else None
                self._emit_attempt(
                    attempt_callback,
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                    outcome="SUCCESS",
                    status_code=status_code,
                    started_at=attempt_started_at,
                    finished_at=attempt_finished_at,
                    duration_seconds=attempt_duration,
                    bytes_downloaded=size,
                    expected_bytes=remote_metadata.content_length,
                    throughput_bytes_per_second=attempt_throughput,
                )
                download_finished_at = attempt_finished_at
                download_duration = max(0.0, time.perf_counter() - download_started_perf)
                return DownloadResult(
                    candidate=candidate,
                    path=temporary,
                    bytes_downloaded=size,
                    sha256=sha256,
                    remote_metadata=effective_remote_metadata,
                    attempt_count=attempt,
                    retry_count=attempt - 1,
                    download_started_at=download_started_at,
                    download_finished_at=download_finished_at,
                    download_duration_seconds=download_duration,
                    throughput_bytes_per_second=(
                        size / download_duration if download_duration > 0 else None
                    ),
                )
            except Exception as exc:
                last_error = exc
                retryable = self._is_retryable_transfer_error(exc)
                final = attempt == max_attempts or not retryable
                delay = None
                if not final:
                    delay = self._retry_delay(response, backoff)
                attempt_finished_at = datetime.now(timezone.utc)
                attempt_duration = max(0.0, time.perf_counter() - attempt_started_perf)
                partial_bytes = temporary.stat().st_size if temporary.is_file() else 0
                self._emit_attempt(
                    attempt_callback,
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                    outcome="EXHAUSTED" if final else "RETRY",
                    status_code=(
                        int(response.status_code)
                        if response is not None
                        and getattr(response, "status_code", None) is not None
                        else None
                    ),
                    retry_delay_seconds=delay,
                    error_type=type(exc).__name__,
                    error_message=str(exc)[:2000],
                    started_at=attempt_started_at,
                    finished_at=attempt_finished_at,
                    duration_seconds=attempt_duration,
                    bytes_downloaded=partial_bytes,
                    expected_bytes=remote_metadata.content_length,
                    throughput_bytes_per_second=(
                        partial_bytes / attempt_duration if attempt_duration > 0 else None
                    ),
                )
                temporary.unlink(missing_ok=True)
                if final:
                    setattr(exc, "attempt_count", attempt)
                    setattr(exc, "retry_count", attempt - 1)
                    setattr(exc, "download_started_at", download_started_at)
                    setattr(exc, "download_finished_at", attempt_finished_at)
                    setattr(
                        exc,
                        "download_duration_seconds",
                        max(0.0, time.perf_counter() - download_started_perf),
                    )
                    raise
                time.sleep(float(delay or 0.0))
                backoff = min(
                    backoff * 2, self._config.max_backoff_seconds
                )
            finally:
                if response is not None:
                    response.close()

        # Defensive fallback; the loop always returns or raises.
        if last_error is not None:
            raise last_error
        raise DownloadError("La descarga terminó sin resultado")

    def _assert_disk_space(self, remote_metadata: RemoteMetadata) -> None:
        expected_size = remote_metadata.content_length or 0
        free = self._storage.free_space_bytes()
        required = (
            max(expected_size, self._config.minimum_file_size_bytes)
            + self._storage.minimum_free_space_bytes
        )
        if free < required:
            raise InsufficientDiskSpaceError(
                f"Espacio insuficiente: libres={free}, requeridos={required}"
            )

    def _validate_download(
        self,
        temporary: Path,
        *,
        size: int,
        content_type: str,
        expected_size: int | None,
    ) -> None:
        if size < self._config.minimum_file_size_bytes:
            raise DownloadError(
                f"Archivo incompleto: {size} bytes; mínimo "
                f"{self._config.minimum_file_size_bytes}"
            )
        if "text/html" in content_type:
            raise DownloadError("El servidor devolvió HTML en lugar de Parquet")
        if self._config.validate_parquet_signature and not has_parquet_signature(
            temporary
        ):
            raise DownloadError("Firma Parquet inválida después de la descarga")
        if expected_size is not None and size != expected_size:
            raise DownloadError(
                f"Tamaño descargado {size} distinto de Content-Length {expected_size}"
            )

    def _retry_delay(self, response: Any | None, fallback: float) -> float:
        if response is not None:
            try:
                return min(
                    float(HttpClient._retry_delay(response, fallback)),
                    self._config.max_backoff_seconds,
                )
            except Exception:
                pass
        return min(float(fallback), self._config.max_backoff_seconds)

    def _is_retryable_transfer_error(self, error: Exception) -> bool:
        if isinstance(error, requests.HTTPError) and error.response is not None:
            return error.response.status_code in self.DOWNLOAD_RETRYABLE_STATUS_CODES
        # Streaming failures commonly surface as ChunkedEncodingError,
        # ConnectionError or Timeout after headers were already accepted.
        return isinstance(
            error,
            (
                requests.Timeout,
                requests.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
            ),
        )

    @staticmethod
    def _emit_attempt(
        callback: Callable[[dict[str, Any]], None] | None,
        *,
        attempt_number: int,
        max_attempts: int,
        outcome: str,
        status_code: int | None,
        retry_delay_seconds: float | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        duration_seconds: float | None = None,
        bytes_downloaded: int | None = None,
        expected_bytes: int | None = None,
        throughput_bytes_per_second: float | None = None,
    ) -> None:
        if callback is None:
            return
        callback(
            {
                "attempt_number": attempt_number,
                "max_attempts": max_attempts,
                "outcome": outcome,
                "status_code": status_code,
                "retry_delay_seconds": retry_delay_seconds,
                "error_type": error_type,
                "error_message": error_message,
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_seconds": duration_seconds,
                "bytes_downloaded": bytes_downloaded,
                "expected_bytes": expected_bytes,
                "throughput_bytes_per_second": throughput_bytes_per_second,
            }
        )
