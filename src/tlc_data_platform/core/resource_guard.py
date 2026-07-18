from __future__ import annotations

import shutil
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, TypeVar

from tlc_data_platform.audit.parquet_metrics import directory_size
from tlc_data_platform.core.exceptions import SparkTemporarySpaceError

T = TypeVar("T")


class SparkResourceGuard:
    """Stops a Spark action before temporary spill can consume the host disk.

    `spark.local.dir` is bind-mounted under data/tmp/spark. A small monitor checks
    both directory growth and remaining free space. When either limit is crossed,
    all Spark jobs are cancelled and the caller receives a clear exception.
    """

    def __init__(
        self,
        spark: Any,
        local_dir: Path,
        *,
        max_temp_bytes: int,
        minimum_free_space_bytes: int,
        poll_seconds: float = 2.0,
    ) -> None:
        self._spark = spark
        self._local_dir = local_dir
        self._max_temp_bytes = max_temp_bytes
        self._minimum_free_space_bytes = minimum_free_space_bytes
        self._poll_seconds = poll_seconds
        self._stop = threading.Event()
        self._reason: str | None = None

    def run(self, action: Callable[[], T]) -> T:
        self._preflight()
        self._stop.clear()
        self._reason = None
        monitor = threading.Thread(target=self._monitor, daemon=True)
        monitor.start()
        try:
            result = action()
            if self._reason:
                raise SparkTemporarySpaceError(self._reason)
            return result
        finally:
            self._stop.set()
            monitor.join(timeout=self._poll_seconds * 2)

    def _preflight(self) -> None:
        self._local_dir.mkdir(parents=True, exist_ok=True)
        free = shutil.disk_usage(self._local_dir).free
        if free < self._minimum_free_space_bytes:
            raise SparkTemporarySpaceError(
                "Espacio libre insuficiente antes de iniciar Spark: "
                f"free={free}, minimum={self._minimum_free_space_bytes}"
            )
        used = directory_size(self._local_dir)
        if used > self._max_temp_bytes:
            raise SparkTemporarySpaceError(
                "El directorio temporal Spark ya supera el límite: "
                f"used={used}, maximum={self._max_temp_bytes}"
            )

    def _monitor(self) -> None:
        while not self._stop.wait(self._poll_seconds):
            try:
                used = directory_size(self._local_dir)
                free = shutil.disk_usage(self._local_dir).free
            except OSError:
                continue
            if used > self._max_temp_bytes:
                self._reason = (
                    "Spark fue cancelado para proteger el disco: "
                    f"temporales={used} > límite={self._max_temp_bytes}."
                )
            elif free < self._minimum_free_space_bytes:
                self._reason = (
                    "Spark fue cancelado para conservar espacio libre: "
                    f"free={free} < mínimo={self._minimum_free_space_bytes}."
                )
            if self._reason:
                try:
                    self._spark.sparkContext.cancelAllJobs()
                except Exception:
                    pass
                return


def reset_spark_local_dir(path: Path) -> None:
    """Removes only layer-owned Spark spill from a previous stopped execution."""
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def cleanup_stale_spark_dirs(root: Path, *, older_than_hours: int = 24) -> None:
    """Removes only abandoned Spark-run directories older than the safety window."""
    if not root.is_dir():
        return
    cutoff = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
    for child in root.iterdir():
        if not child.is_dir():
            continue
        try:
            modified = datetime.fromtimestamp(child.stat().st_mtime, timezone.utc)
        except OSError:
            continue
        if modified < cutoff:
            shutil.rmtree(child, ignore_errors=True)
