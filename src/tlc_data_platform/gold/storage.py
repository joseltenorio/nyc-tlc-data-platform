from __future__ import annotations

import shutil
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tlc_data_platform.audit.parquet_metrics import parquet_files
from tlc_data_platform.core.settings import (
    GoldConfig,
    RunSelection,
    ServiceConfig,
    SilverStorageConfig,
)


@dataclass(frozen=True)
class GoldSourcePartition:
    service: str
    year: int
    month: int
    path: Path

    @property
    def period_id(self) -> str:
        return f"{self.service}/{self.year}-{self.month:02d}"


class GoldStorage:
    """Resolves inputs and publishes Gold directories using recoverable swaps."""

    def __init__(
        self,
        gold: GoldConfig,
        silver: SilverStorageConfig,
        services: dict[str, ServiceConfig],
    ) -> None:
        self.gold = gold
        self.silver = silver
        self.services = services

    def ensure_directories(self) -> None:
        for path in (
            self.gold.storage.gold_root,
            self.gold.storage.temporary_root,
            self.gold.storage.manifests_root,
        ):
            path.mkdir(parents=True, exist_ok=True)
        self.recover_interrupted_promotions()

    def recover_interrupted_promotions(self) -> None:
        root = self.gold.storage.gold_root
        if not root.is_dir():
            return
        for backup in sorted(root.rglob("*.previous-*")):
            if not backup.is_dir():
                continue
            destination_name = backup.name.split(".previous-", 1)[0]
            destination = backup.with_name(destination_name)
            if destination.exists():
                shutil.rmtree(backup, ignore_errors=True)
            else:
                backup.rename(destination)

    def cleanup_execution(self, execution_id: str) -> None:
        shutil.rmtree(self.gold.storage.temporary_root / execution_id, ignore_errors=True)

    def cleanup_stale_temporary(self, *, older_than_hours: int = 24) -> None:
        root = self.gold.storage.temporary_root
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

    def dimension_path(self, logical_name: str) -> Path:
        return self.gold.storage.gold_root / self.gold.storage.dimensions_root / self.gold.datasets.dimensions[logical_name]

    def fact_path(self, logical_name: str) -> Path:
        return self.gold.storage.gold_root / self.gold.storage.facts_root / self.gold.datasets.facts[logical_name]

    def mart_path(self, logical_name: str) -> Path:
        return self.gold.storage.gold_root / self.gold.storage.marts_root / self.gold.datasets.marts[logical_name]

    def feature_path(self, logical_name: str) -> Path:
        return self.gold.storage.gold_root / self.gold.storage.features_root / self.gold.datasets.ml_features[logical_name]

    def silver_master_partition(self, service: str, year: int, month: int) -> Path:
        return (
            self.silver.silver_root
            / self.silver.master_dataset
            / f"service_type={service}"
            / f"year={year}"
            / f"month={month:02d}"
        )

    def selected_master_partitions(self, selection: RunSelection) -> tuple[list[GoldSourcePartition], list[str]]:
        available: list[GoldSourcePartition] = []
        missing: list[str] = []
        for service in selection.services:
            for year in range(selection.start_year, selection.end_year + 1):
                for month in selection.months:
                    if not self.period_in_scope(service, year, month):
                        continue
                    path = self.silver_master_partition(service, year, month)
                    item = GoldSourcePartition(service, year, month, path)
                    if self.has_parquet(path):
                        available.append(item)
                    else:
                        missing.append(item.period_id)
        return available, missing

    def selected_master_paths(self, selection: RunSelection) -> tuple[list[Path], list[str]]:
        partitions, missing = self.selected_master_partitions(selection)
        return [item.path for item in partitions], missing

    def applicable_period_count(self, selection: RunSelection) -> int:
        return sum(
            self.period_in_scope(service, year, month)
            for service in selection.services
            for year in range(selection.start_year, selection.end_year + 1)
            for month in selection.months
        )

    def scoped_master_paths(self, start_year: int, end_year: int) -> list[Path]:
        paths: list[Path] = []
        for service in sorted(self.services):
            for year in range(start_year, end_year + 1):
                for month in range(1, 13):
                    if not self.period_in_scope(service, year, month):
                        continue
                    path = self.silver_master_partition(service, year, month)
                    if self.has_parquet(path):
                        paths.append(path)
        return paths

    def scoped_fact_paths(
        self, logical_name: str, start_year: int, end_year: int
    ) -> list[Path]:
        supported_services = {
            "trip_activity": set(self.services),
            "taxi_financial": {"yellow", "green"},
            "hvfhv_operations": {"fhvhv"},
        }[logical_name]
        paths: list[Path] = []
        for service in sorted(supported_services):
            if service not in self.services:
                continue
            for year in range(start_year, end_year + 1):
                for month in range(1, 13):
                    if not self.period_in_scope(service, year, month):
                        continue
                    path = self.fact_partition_path(
                        logical_name, service, year, month
                    )
                    if self.has_parquet(path):
                        paths.append(path)
        return paths

    def period_in_scope(self, service: str, year: int, month: int) -> bool:
        config = self.services[service]
        point = (year, month)
        return (
            point >= (config.available_from.year, config.available_from.month)
            and point >= (config.scope_from.year, config.scope_from.month)
            and point <= (config.scope_to.year, config.scope_to.month)
        )

    def taxi_zone_path(self) -> Path:
        return self.silver.silver_root / self.silver.taxi_zones_dataset

    @staticmethod
    def has_parquet(path: Path) -> bool:
        return bool(parquet_files(path))

    def fact_partition_path(self, logical_name: str, service: str, year: int, month: int) -> Path:
        return (
            self.fact_path(logical_name)
            / f"service_type={service}"
            / f"source_year={year}"
            / f"source_month={month}"
        )

    def write_atomic(self, frame: Any, destination: Path, execution_id: str, logical_name: str) -> Path:
        staging = self._staging_path(execution_id, logical_name)
        shutil.rmtree(staging, ignore_errors=True)
        staging.parent.mkdir(parents=True, exist_ok=True)
        (
            frame.write.mode("overwrite")
            .option("compression", self.gold.execution.parquet_compression)
            .parquet(str(staging))
        )
        self._validate_staging(staging)
        self._promote(staging, destination, execution_id)
        return destination

    def write_fact_partition_atomic(
        self,
        frame: Any,
        logical_name: str,
        service: str,
        year: int,
        month: int,
        execution_id: str,
    ) -> Path:
        destination = self.fact_partition_path(logical_name, service, year, month)
        staging = self._staging_path(
            execution_id, f"fact-{logical_name}-{service}-{year}-{month:02d}"
        )
        shutil.rmtree(staging, ignore_errors=True)
        staging.parent.mkdir(parents=True, exist_ok=True)
        data = frame.drop("service_type", "source_year", "source_month")
        (
            data.write.mode("overwrite")
            .option("compression", self.gold.execution.parquet_compression)
            .parquet(str(staging))
        )
        self._validate_staging(staging, allow_empty=False)
        self._promote(staging, destination, execution_id)
        return destination

    def _staging_path(self, execution_id: str, logical_name: str) -> Path:
        return self.gold.storage.temporary_root / execution_id / logical_name

    @staticmethod
    def _validate_staging(path: Path, *, allow_empty: bool = False) -> None:
        if not path.is_dir() or not (path / "_SUCCESS").exists():
            raise RuntimeError(f"Spark no completó la escritura temporal: {path}")
        if not allow_empty and not parquet_files(path):
            raise RuntimeError(f"La escritura temporal no contiene Parquet: {path}")

    @staticmethod
    def _promote(staging: Path, destination: Path, execution_id: str) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        backup = destination.with_name(f"{destination.name}.previous-{execution_id}")
        shutil.rmtree(backup, ignore_errors=True)
        had_previous = destination.exists()
        try:
            if had_previous:
                destination.rename(backup)
            staging.rename(destination)
            shutil.rmtree(backup, ignore_errors=True)
        except Exception:
            if destination.exists():
                shutil.rmtree(destination, ignore_errors=True)
            if had_previous and backup.exists():
                backup.rename(destination)
            raise
