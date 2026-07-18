from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@lru_cache(maxsize=1)
def load_dashboard_config() -> dict[str, Any]:
    path = PROJECT_ROOT / "config" / "dashboard.yml"
    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError("config/dashboard.yml debe contener un objeto YAML raíz.")
    return raw


def project_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def gold_config() -> dict[str, Any]:
    path = PROJECT_ROOT / "config" / "gold.yml"
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def ml_config() -> dict[str, Any]:
    path = PROJECT_ROOT / "config" / "ml.yml"
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def audit_config() -> dict[str, Any]:
    path = PROJECT_ROOT / "config" / "audit.yml"
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}
