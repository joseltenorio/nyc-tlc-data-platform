from __future__ import annotations

import os
from pathlib import Path

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


class FileDownloader:
    DOWNLOAD_RETRYABLE_STATUS_CODES = (
        HttpClient.RETRYABLE_STATUS_CODES | {403}
    )

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
    ) -> DownloadResult:
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

        temporary = self._storage.temporary_path(candidate, execution_id)
        temporary.parent.mkdir(parents=True, exist_ok=True)
        temporary.unlink(missing_ok=True)

        response = self._http.request(
            "GET",
            candidate.url,
            stream=True,
            allow_redirects=True,
            retryable_status_codes=self.DOWNLOAD_RETRYABLE_STATUS_CODES,
        )
        try:
            response.raise_for_status()
            content_type = (response.headers.get("Content-Type") or "").lower()
            with temporary.open("wb") as handle:
                for chunk in response.iter_content(self._config.chunk_size_bytes):
                    if chunk:
                        handle.write(chunk)
                handle.flush()
                os.fsync(handle.fileno())
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        finally:
            response.close()

        size = temporary.stat().st_size
        if size < self._config.minimum_file_size_bytes:
            temporary.unlink(missing_ok=True)
            raise DownloadError(
                f"Archivo incompleto: {size} bytes; mínimo {self._config.minimum_file_size_bytes}"
            )
        if "text/html" in content_type:
            temporary.unlink(missing_ok=True)
            raise DownloadError("El servidor devolvió HTML en lugar de Parquet")
        if self._config.validate_parquet_signature and not has_parquet_signature(temporary):
            temporary.unlink(missing_ok=True)
            raise DownloadError("Firma Parquet inválida después de la descarga")
        if remote_metadata.content_length is not None and size != remote_metadata.content_length:
            temporary.unlink(missing_ok=True)
            raise DownloadError(
                f"Tamaño descargado {size} distinto de Content-Length {remote_metadata.content_length}"
            )

        sha256 = calculate_sha256(temporary) if self._config.calculate_sha256 else ""
        return DownloadResult(
            candidate=candidate,
            path=temporary,
            bytes_downloaded=size,
            sha256=sha256,
            remote_metadata=remote_metadata,
        )
