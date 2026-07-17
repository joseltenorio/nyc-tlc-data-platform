from __future__ import annotations

from dataclasses import dataclass

from tlc_data_platform.audit.silver_execution_repository import SilverExecutionRepository
from tlc_data_platform.audit.silver_file_registry_repository import SilverFileRegistryRepository
from tlc_data_platform.audit.silver_quality_repository import SilverQualityRepository
from tlc_data_platform.audit.silver_reconciliation_repository import SilverReconciliationRepository


@dataclass(frozen=True)
class SilverAuditRepositories:
    executions: SilverExecutionRepository
    registry: SilverFileRegistryRepository
    quality: SilverQualityRepository
    reconciliations: SilverReconciliationRepository
