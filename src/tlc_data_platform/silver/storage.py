from __future__ import annotations

import shutil
from pathlib import Path

from tlc_data_platform.core.settings import SilverStorageConfig
from tlc_data_platform.silver.models import SilverSourceFile


class SilverStorage:
    def __init__(self, config: SilverStorageConfig) -> None:
        self.config = config

    def ensure_directories(self) -> None:
        roots = [
            self.config.silver_root,
            self.config.temporary_root,
            self.config.manifests_root,
        ]
        for root in roots:
            root.mkdir(parents=True, exist_ok=True)
        self.recover_interrupted_promotions()

    def recover_interrupted_promotions(self) -> None:
        """Restores a previous partition after a host/process interruption."""
        root = self.config.silver_root
        if not root.is_dir():
            return
        for backup in sorted(root.rglob("*.previous")):
            if not backup.is_dir():
                continue
            destination = backup.with_name(backup.name.removesuffix(".previous"))
            if destination.exists():
                shutil.rmtree(backup, ignore_errors=True)
            else:
                backup.replace(destination)

    def curated_partition(self, source: SilverSourceFile) -> Path:
        dataset = self.config.datasets[source.service]
        return self.config.silver_root / dataset / f"year={source.year}" / f"month={source.month:02d}"

    def rejected_partition(self, source: SilverSourceFile) -> Path:
        return (
            self.config.silver_root
            / self.config.rejected_dataset
            / f"service_type={source.service}"
            / f"year={source.year}"
            / f"month={source.month:02d}"
        )

    def master_partition(self, source: SilverSourceFile) -> Path:
        return (
            self.config.silver_root
            / self.config.master_dataset
            / f"service_type={source.service}"
            / f"year={source.year}"
            / f"month={source.month:02d}"
        )

    def reference_path(self, name: str) -> Path:
        dataset = {
            "taxi_zones": self.config.taxi_zones_dataset,
            "base_lookup": self.config.base_lookup_dataset,
        }[name]
        return self.config.silver_root / dataset

    def references_exist(self) -> bool:
        return all(self._has_parquet(self.reference_path(name)) for name in ("taxi_zones", "base_lookup"))

    def temp_partition(self, execution_id: str, source: SilverSourceFile, kind: str) -> Path:
        return (
            self.config.temporary_root
            / execution_id
            / kind
            / source.service
            / f"year={source.year}"
            / f"month={source.month:02d}"
        )

    @staticmethod
    def _has_parquet(path: Path) -> bool:
        return path.is_dir() and any(path.rglob("*.parquet"))

    @staticmethod
    def _is_complete_dataset(path: Path) -> bool:
        return path.is_dir() and (path / "_SUCCESS").is_file()

    @staticmethod
    def _replace_many(pairs: list[tuple[Path, Path]]) -> None:
        """Promotes multiple directories as one recoverable filesystem transaction.

        A crash cannot be made fully atomic across multiple directories, but any Python-level
        failure rolls every destination back to its previous version.
        """
        if not pairs:
            return
        for temp_path, _ in pairs:
            if not temp_path.is_dir():
                raise FileNotFoundError(f"No existe la salida temporal: {temp_path}")
            if not (temp_path / "_SUCCESS").is_file():
                raise RuntimeError(f"Spark no completó la salida temporal: {temp_path}")

        backups: dict[Path, Path] = {}
        promoted: list[Path] = []
        try:
            for _, final_path in pairs:
                final_path.parent.mkdir(parents=True, exist_ok=True)
                backup = final_path.with_name(final_path.name + ".previous")
                if backup.exists():
                    shutil.rmtree(backup)
                if final_path.exists():
                    final_path.replace(backup)
                    backups[final_path] = backup

            for temp_path, final_path in pairs:
                temp_path.replace(final_path)
                promoted.append(final_path)
        except Exception:
            for final_path in reversed(promoted):
                if final_path.exists():
                    shutil.rmtree(final_path)
            for final_path, backup in backups.items():
                if backup.exists() and not final_path.exists():
                    backup.replace(final_path)
            raise
        else:
            for backup in backups.values():
                if backup.exists():
                    shutil.rmtree(backup)

    @staticmethod
    def _replace_directory(temp_path: Path, final_path: Path) -> None:
        SilverStorage._replace_many([(temp_path, final_path)])

    def promote(
        self,
        execution_id: str,
        source: SilverSourceFile,
        include_master: bool,
    ) -> tuple[Path, Path, Path | None]:
        curated = self.curated_partition(source)
        rejected = self.rejected_partition(source)
        master = self.master_partition(source) if include_master else None
        pairs = [
            (self.temp_partition(execution_id, source, "curated"), curated),
            (self.temp_partition(execution_id, source, "rejected"), rejected),
        ]
        if include_master and master is not None:
            pairs.append((self.temp_partition(execution_id, source, "master"), master))
        self._replace_many(pairs)
        return curated, rejected, master

    def promote_references(self, taxi_temp: Path, base_temp: Path) -> tuple[Path, Path]:
        taxi_final = self.reference_path("taxi_zones")
        base_final = self.reference_path("base_lookup")
        self._replace_many([(taxi_temp, taxi_final), (base_temp, base_final)])
        return taxi_final, base_final

    def outputs_exist(self, source: SilverSourceFile, include_master: bool) -> bool:
        paths = [self.curated_partition(source), self.rejected_partition(source)]
        if include_master:
            paths.append(self.master_partition(source))
        return all(self._is_complete_dataset(path) for path in paths)

    def cleanup_execution(self, execution_id: str) -> None:
        root = self.config.temporary_root / execution_id
        if root.exists():
            shutil.rmtree(root)
