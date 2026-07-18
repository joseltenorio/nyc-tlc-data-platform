from __future__ import annotations

import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from tlc_data_platform.audit.parquet_metrics import parquet_files
from tlc_data_platform.core.settings import GoldConfig, MLConfig


class MLStorage:
    """Resolves Gold feature inputs and atomically publishes model outputs."""

    FEATURE_LOGICAL_NAMES = {
        "forecast": "zone_hourly_demand",
        "segmentation": "zone_profiles",
        "wait-risk": "hvfhv_wait_risk",
    }

    def __init__(self, ml: MLConfig, gold: GoldConfig) -> None:
        self.ml = ml
        self.gold = gold

    def ensure_directories(self) -> None:
        for path in (
            self.ml.storage.ml_root,
            self.ml.storage.model_root,
            self.ml.storage.temporary_root,
            self.ml.storage.manifests_root,
        ):
            path.mkdir(parents=True, exist_ok=True)
        self.recover_interrupted_promotions()

    def recover_interrupted_promotions(self) -> None:
        root = self.ml.storage.ml_root
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

    def cleanup_execution(self, run_id: str) -> None:
        shutil.rmtree(self.ml.storage.temporary_root / run_id, ignore_errors=True)

    def cleanup_stale_temporary(self, *, older_than_hours: int = 24) -> None:
        root = self.ml.storage.temporary_root
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

    def feature_path(self, model_name: str) -> Path:
        logical = self.FEATURE_LOGICAL_NAMES[model_name]
        return (
            self.gold.storage.gold_root
            / self.gold.storage.features_root
            / self.gold.datasets.ml_features[logical]
        )

    def output_path(self, dataset_name: str) -> Path:
        return self.ml.storage.ml_root / dataset_name

    def model_path(self, model_name: str, model_id: str) -> Path:
        return self.ml.storage.model_root / model_name / model_id

    @staticmethod
    def has_parquet(path: Path) -> bool:
        return bool(parquet_files(path))

    def write_atomic(self, frame: Any, path: Path, run_id: str, logical_name: str) -> Path:
        staging = self.ml.storage.temporary_root / run_id / logical_name
        shutil.rmtree(staging, ignore_errors=True)
        staging.parent.mkdir(parents=True, exist_ok=True)
        (
            frame.write.mode("overwrite")
            .option("compression", self.ml.execution.parquet_compression)
            .parquet(str(staging))
        )
        if not (staging / "_SUCCESS").exists() or not parquet_files(staging):
            raise RuntimeError(f"Salida ML temporal incompleta: {staging}")
        self._promote(staging, path, run_id)
        return path

    @staticmethod
    def _promote(staging: Path, destination: Path, run_id: str) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        backup = destination.with_name(f"{destination.name}.previous-{run_id}")
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
