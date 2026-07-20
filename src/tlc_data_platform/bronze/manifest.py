from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from tlc_data_platform.bronze.models import AvailabilityRecord, ExecutionSummary, FileOutcome


class JsonEncoder(json.JSONEncoder):
    def default(self, value: Any) -> Any:
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Path):
            return str(value)
        if is_dataclass(value):
            return asdict(value)
        return super().default(value)


class ManifestWriter:
    def __init__(self, root: Path, execution_id: str) -> None:
        self.path = root / f"{execution_id}.json"
        self._outcomes: list[FileOutcome] = []
        self._availability: list[AvailabilityRecord] = []

    def add_outcome(self, outcome: FileOutcome) -> None:
        self._outcomes.append(outcome)

    def set_availability(self, availability: list[AvailabilityRecord]) -> None:
        self._availability = availability

    def write(self, summary: ExecutionSummary) -> Path:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "manifest_schema_version": "1.0",
            "layer": "bronze",
            "manifest_type": "pipeline_execution",
            "summary": summary.to_dict(),
            "availability": [item.to_dict() for item in self._availability],
            "files": [item.to_dict() for item in self._outcomes],
        }
        temporary = self.path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(payload, cls=JsonEncoder, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary.replace(self.path)
        return self.path