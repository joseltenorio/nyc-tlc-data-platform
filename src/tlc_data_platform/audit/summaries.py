from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from tlc_data_platform.audit.availability_repository import AvailabilityRepository
from tlc_data_platform.audit.execution_repository import ExecutionRepository
from tlc_data_platform.audit.file_registry_repository import FileRegistryRepository
from tlc_data_platform.audit.file_version_repository import FileVersionRepository


@dataclass(frozen=True)
class AuditRepositories:
    executions: ExecutionRepository
    availability: AvailabilityRepository
    registry: FileRegistryRepository
    versions: FileVersionRepository
    unified: Any | None = None
