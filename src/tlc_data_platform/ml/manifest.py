"""Genera el manifiesto JSON de cada corrida de entrenamiento ML."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any


class _Encoder(json.JSONEncoder):
    def default(self, value: Any) -> Any:
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Path):
            return str(value)
        return super().default(value)


class MLManifestWriter:
    """Writes one consistent manifest under data/manifests/ml/."""

    def __init__(self, root: Path, run_id: str) -> None:
        root.mkdir(parents=True, exist_ok=True)
        self.path = root / f"{run_id}.json"

    def write(self, payload: Any) -> Path:
        document = {
            "manifest_schema_version": "1.0",
            "layer": "ml",
            "manifest_type": "pipeline_execution",
            "summary": payload.to_dict(),
        }
        temp = self.path.with_suffix(".json.part")
        temp.write_text(
            json.dumps(document, cls=_Encoder, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp.replace(self.path)
        return self.path
