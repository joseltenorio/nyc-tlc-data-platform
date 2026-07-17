from __future__ import annotations

import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

from tlc_data_platform.bronze.models import FileCandidate
from tlc_data_platform.core.settings import StorageConfig

TEMPORARY_FILE_PATTERN = re.compile(
    r"^(?P<file_name>.+\.parquet)\.(?P<execution_id>[^.]+)\.part$"
)


class BronzeStorage:
    def __init__(self, config: StorageConfig) -> None:
        self._config = config

    def ensure_directories(self) -> None:
        for path in (
            self._config.bronze_root,
            self._config.versions_root,
            self._config.temporary_root,
            self._config.manifests_root,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def final_path(self, candidate: FileCandidate) -> Path:
        return (
            self._config.bronze_root
            / candidate.service
            / f"year={candidate.year}"
            / f"month={candidate.month:02d}"
            / candidate.file_name
        )

    def temporary_path(self, candidate: FileCandidate, execution_id: str) -> Path:
        safe_execution = execution_id.replace(":", "-")
        return self._config.temporary_root / f"{candidate.file_name}.{safe_execution}.part"

    def version_path(self, candidate: FileCandidate, sha256: str) -> Path:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        stem = candidate.file_name.removesuffix(".parquet")
        return (
            self._config.versions_root
            / candidate.service
            / f"year={candidate.year}"
            / f"month={candidate.month:02d}"
            / f"{stem}.{timestamp}.{sha256[:12]}.parquet"
        )

    def promote(
        self,
        temporary: Path,
        candidate: FileCandidate,
        previous_sha256: str | None,
        new_sha256: str,
    ) -> tuple[Path, Path | None]:
        destination = self.final_path(candidate)
        destination.parent.mkdir(parents=True, exist_ok=True)
        archived: Path | None = None

        if destination.exists() and previous_sha256 != new_sha256:
            archived = self.version_path(candidate, previous_sha256 or "unknown")
            archived.parent.mkdir(parents=True, exist_ok=True)
            os.replace(destination, archived)

        os.replace(temporary, destination)
        return destination, archived

    def discard_temporary(self, candidate: FileCandidate, execution_id: str) -> None:
        self.temporary_path(candidate, execution_id).unlink(missing_ok=True)

    def temporary_entries(self) -> list[tuple[Path, str]]:
        entries: list[tuple[Path, str]] = []
        self._config.temporary_root.mkdir(parents=True, exist_ok=True)
        for path in self._config.temporary_root.glob("*.part"):
            match = TEMPORARY_FILE_PATTERN.match(path.name)
            if match is None:
                continue
            entries.append((path, match.group("execution_id")))
        return entries

    def discard_temporary_path(self, path: Path) -> None:
        path.unlink(missing_ok=True)

    def discard_temporary_for_execution(self, execution_id: str) -> int:
        removed = 0
        for path, owner_execution_id in self.temporary_entries():
            if owner_execution_id != execution_id:
                continue
            path.unlink(missing_ok=True)
            removed += 1
        return removed

    def free_space_bytes(self) -> int:
        target = self._config.temporary_root
        target.mkdir(parents=True, exist_ok=True)
        return shutil.disk_usage(target).free

    @property
    def minimum_free_space_bytes(self) -> int:
        return self._config.minimum_free_space_bytes