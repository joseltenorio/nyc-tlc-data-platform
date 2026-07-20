from __future__ import annotations

import json
from pathlib import Path

from tlc_data_platform.bronze.manifest import JsonEncoder
from tlc_data_platform.silver.models import SilverExecutionSummary, SilverFileOutcome, SilverPeriodState


class SilverManifestWriter:
    def __init__(self, root: Path, execution_id: str) -> None:
        self.path = root / f"{execution_id}.json"
        self.outcomes: list[SilverFileOutcome] = []
        self.states: list[SilverPeriodState] = []

    def add(self, outcome: SilverFileOutcome) -> None:
        self.outcomes.append(outcome)

    def set_states(self, states: list[SilverPeriodState]) -> None:
        self.states = states

    def write(self, summary: SilverExecutionSummary) -> Path:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "manifest_schema_version": "1.0",
            "layer": "silver",
            "manifest_type": "pipeline_execution",
            "summary": summary.to_dict(),
            "period_states": [state.to_dict() for state in self.states],
            "files": [outcome.to_dict() for outcome in self.outcomes],
        }
        temp = self.path.with_suffix(".json.tmp")
        temp.write_text(json.dumps(payload, cls=JsonEncoder, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(self.path)
        return self.path
