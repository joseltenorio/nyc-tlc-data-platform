from __future__ import annotations

import json
import math
import os
import threading
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from tlc_data_platform.core.settings import AuditFilesystemConfig


_LOCKS: dict[str, threading.RLock] = {}
_LOCKS_GUARD = threading.Lock()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return _json_safe(value.value)
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    as_py = getattr(value, "as_py", None)
    if callable(as_py):
        return _json_safe(as_py())
    scalar = getattr(value, "item", None)
    if callable(scalar):
        return _json_safe(scalar())
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return _json_safe(to_dict())
    raise TypeError(f"Tipo no serializable en auditoría JSONL: {type(value).__name__}")


def _path_lock(path: Path) -> threading.RLock:
    key = str(path.resolve())
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(key, threading.RLock())


class FileAuditSink:
    """Persists append-only audit events and a physical Parquet inventory.

    MongoDB remains the operational store, while these JSONL files are an
    independent, dashboard-readable evidence trail. Every record is produced
    from pipeline facts or a physical filesystem scan; no synthetic values are
    introduced.
    """

    SCHEMA_VERSION = "1.0"

    def __init__(self, config: AuditFilesystemConfig) -> None:
        self._config = config
        self._event_files = {
            "pipeline_run": config.pipeline_runs_file,
            "dataset_event": config.dataset_events_file,
            "quality_event": config.quality_events_file,
            "coverage_snapshot": config.coverage_snapshots_file,
            "download_attempt": config.download_attempts_file,
        }

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    def append(self, event_type: str, layer: str, document: dict[str, Any]) -> None:
        if not self.enabled:
            return
        file_name = self._event_files[event_type]
        normalized_layer = (layer or "unknown").strip().lower()
        path = self._config.root / normalized_layer / file_name
        payload = {
            "audit_schema_version": self.SCHEMA_VERSION,
            "event_type": event_type,
            "written_at": utc_now(),
            **document,
        }
        payload["layer"] = normalized_layer
        line = json.dumps(
            _json_safe(payload),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        with _path_lock(path):
            with path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(line + "\n")
                handle.flush()

    def refresh_inventory(
        self,
        *,
        execution_id: str,
        trigger_layer: str,
        status: str,
    ) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        captured_at = utc_now()
        layers = [
            self._scan_layer(layer, root)
            for layer, root in sorted(self._config.layer_roots.items())
        ]
        snapshot = {
            "audit_schema_version": self.SCHEMA_VERSION,
            "snapshot_id": str(uuid4()),
            "execution_id": execution_id,
            "trigger_layer": trigger_layer.lower(),
            "status": status.upper(),
            "captured_at": captured_at,
            "layers": layers,
            "totals": {
                "parquet_files": sum(item["parquet_files"] for item in layers),
                "bytes_on_disk": sum(item["bytes_on_disk"] for item in layers),
                "datasets": sum(item["dataset_count"] for item in layers),
                "scan_errors": sum(item["scan_error_count"] for item in layers),
            },
        }
        inventory_dir = self._config.root / "inventory"
        history_path = inventory_dir / self._config.inventory_snapshots_file
        current_path = inventory_dir / self._config.inventory_current_file
        inventory_dir.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(
            _json_safe(snapshot),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        with _path_lock(history_path):
            with history_path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(serialized + "\n")
                handle.flush()
        temporary = current_path.with_suffix(current_path.suffix + ".tmp")
        with _path_lock(current_path):
            temporary.write_text(
                json.dumps(
                    _json_safe(snapshot),
                    ensure_ascii=False,
                    allow_nan=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            os.replace(temporary, current_path)
        return snapshot

    @staticmethod
    def _scan_layer(layer: str, root: Path) -> dict[str, Any]:
        files: list[Path] = []
        errors: list[dict[str, str]] = []
        if root.is_file() and root.suffix.lower() == ".parquet":
            files = [root]
        elif root.is_dir():
            try:
                files = sorted(item for item in root.rglob("*.parquet") if item.is_file())
            except OSError as exc:
                errors.append({"path": str(root), "error": str(exc)[:1000]})

        datasets: dict[str, dict[str, Any]] = {}
        total_bytes = 0
        latest_modified_ns = 0
        for file_path in files:
            try:
                stat = file_path.stat()
            except OSError as exc:
                errors.append({"path": str(file_path), "error": str(exc)[:1000]})
                continue
            total_bytes += stat.st_size
            latest_modified_ns = max(latest_modified_ns, stat.st_mtime_ns)
            dataset_name = FileAuditSink._dataset_name(layer, root, file_path)
            bucket = datasets.setdefault(
                dataset_name,
                {"dataset_name": dataset_name, "parquet_files": 0, "bytes_on_disk": 0},
            )
            bucket["parquet_files"] += 1
            bucket["bytes_on_disk"] += stat.st_size

        latest_modified_at = None
        if latest_modified_ns:
            latest_modified_at = datetime.fromtimestamp(
                latest_modified_ns / 1_000_000_000, tz=timezone.utc
            )
        return {
            "layer": layer.lower(),
            "root": str(root),
            "root_exists": root.exists(),
            "parquet_files": sum(item["parquet_files"] for item in datasets.values()),
            "bytes_on_disk": total_bytes,
            "dataset_count": len(datasets),
            "latest_modified_at": latest_modified_at,
            "scan_error_count": len(errors),
            "scan_errors": errors,
            "datasets": sorted(datasets.values(), key=lambda item: item["dataset_name"]),
        }

    @staticmethod
    def _dataset_name(layer: str, root: Path, file_path: Path) -> str:
        try:
            parts = file_path.relative_to(root).parts
        except ValueError:
            return root.name
        if not parts:
            return root.name
        normalized_layer = layer.lower()
        if normalized_layer == "bronze":
            if parts[0] == "trip_records" and len(parts) >= 2:
                return parts[1]
            if parts[0] == "versions" and len(parts) >= 3:
                return f"versions/{parts[2]}"
            if parts[0] == "reference" and len(parts) >= 2:
                return f"reference/{parts[1]}"
        if normalized_layer == "gold" and len(parts) >= 2:
            return parts[1]
        first = parts[0]
        return root.name if "=" in first else first
