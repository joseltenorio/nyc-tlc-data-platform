from __future__ import annotations

from pathlib import Path
from typing import Any

from tlc_data_platform.core.settings import AppConfig, RunSelection
from tlc_data_platform.silver.models import SilverPeriodState, SilverSourceFile


class SilverSourceCatalog:
    def __init__(self, config: AppConfig, bronze_registry: Any | None) -> None:
        self._config = config
        self._registry = bronze_registry

    def list(self, selection: RunSelection) -> tuple[list[SilverSourceFile], list[SilverPeriodState]]:
        sources: list[SilverSourceFile] = []
        states: list[SilverPeriodState] = []
        for service in selection.services:
            available_from = self._config.services[service].available_from
            for year in range(selection.start_year, selection.end_year + 1):
                for month in selection.months:
                    if (year, month) < (available_from.year, available_from.month):
                        states.append(SilverPeriodState(service, year, month, "NOT_APPLICABLE"))
                        continue
                    doc = self._registry.find_one(
                        {"service": service, "year": year, "month": month},
                        {"_id": 0},
                    ) if self._registry is not None else None
                    source = self._from_registry(service, year, month, doc)
                    if source is None and not self._config.silver.execution.require_bronze_ready_registry:
                        source = self._from_filesystem(service, year, month)
                    if source is None:
                        detail = "No existe file_registry READY o el archivo físico no está disponible"
                        states.append(SilverPeriodState(service, year, month, "BRONZE_NOT_READY", detail=detail))
                    else:
                        sources.append(source)
                        states.append(
                            SilverPeriodState(
                                service,
                                year,
                                month,
                                "BRONZE_READY",
                                source_path=str(source.path),
                                source_sha256=source.source_sha256,
                            )
                        )
        return sources, states

    def _from_registry(self, service: str, year: int, month: int, doc: dict[str, Any] | None) -> SilverSourceFile | None:
        if not doc or doc.get("status") != "READY":
            return None
        current = doc.get("current") or {}
        raw_path = current.get("local_path") or doc.get("local_path")
        if not raw_path:
            return None
        path = Path(raw_path)
        if not path.is_file():
            return None
        validation = current.get("validation") or (doc.get("current") or {}).get("validation") or {}
        return SilverSourceFile(
            service=service,
            year=year,
            month=month,
            path=path,
            source_sha256=current.get("sha256") or doc.get("sha256"),
            bronze_execution_id=current.get("execution_id") or doc.get("last_execution_id"),
            bronze_num_rows=validation.get("parquet_num_rows"),
            bronze_registry_status="READY",
        )

    def _from_filesystem(self, service: str, year: int, month: int) -> SilverSourceFile | None:
        root = self._config.storage.bronze_root / service / f"year={year}" / f"month={month:02d}"
        files = sorted(root.glob("*.parquet")) if root.is_dir() else []
        if len(files) != 1:
            return None
        return SilverSourceFile(service, year, month, files[0], None, None, None, "FILESYSTEM_ONLY")
