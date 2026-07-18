"""Serializa el resumen final de Gold como manifiesto JSON trazable."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any


class _Encoder(json.JSONEncoder):
    def default(self, value: Any) -> Any:
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        return super().default(value)


class GoldManifestWriter:
    """Writes a portable execution summary in addition to MongoDB audit."""

    def __init__(self, root: Path, execution_id: str) -> None:
        root.mkdir(parents=True, exist_ok=True)
        self.path = root / f"gold_{execution_id}.json"

    def write(self, payload: Any) -> None:
        temp = self.path.with_suffix(".json.part")
        temp.write_text(
            json.dumps(payload.to_dict(), cls=_Encoder, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp.replace(self.path)
