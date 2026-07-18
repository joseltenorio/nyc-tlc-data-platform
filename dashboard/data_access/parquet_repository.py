from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from dashboard.config import gold_config, load_dashboard_config, ml_config, project_path


def _dataset_signature(path: Path) -> tuple[int, int]:
    files = list(path.rglob("*.parquet")) if path.is_dir() else []
    if not files:
        return (0, 0)
    return (len(files), max(file.stat().st_mtime_ns for file in files))


@st.cache_data(ttl=300, show_spinner=False)
def _read_parquet_cached(path_text: str, signature: tuple[int, int]) -> pd.DataFrame:
    del signature
    path = Path(path_text)
    parquet_files = list(path.rglob("*.parquet")) if path.is_dir() else []
    if not parquet_files:
        return pd.DataFrame()
    glob = (path / "**" / "*.parquet").as_posix().replace("'", "''")
    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute("PRAGMA threads=4")
        return connection.execute(
            f"SELECT * FROM read_parquet('{glob}', union_by_name=true, hive_partitioning=true)"
        ).fetch_df()
    finally:
        connection.close()


@dataclass(frozen=True)
class DatasetLocation:
    logical_name: str
    physical_name: str
    path: Path


class AnalyticsRepository:
    """Reads only persisted Gold/ML outputs; it never starts Spark."""

    def __init__(self) -> None:
        dashboard = load_dashboard_config()
        self._gold_root = project_path(dashboard["paths"]["gold_root"])
        self._ml_root = project_path(dashboard["paths"]["ml_root"])
        self._gold = gold_config()
        self._ml = ml_config()

    def read_mart(self, logical_name: str) -> pd.DataFrame:
        datasets = self._gold.get("datasets", {}).get("marts", {})
        physical = datasets.get(logical_name, logical_name)
        return self._read(self._gold_root / self._gold["storage"]["marts_root"] / physical)

    def read_fact(self, logical_name: str) -> pd.DataFrame:
        datasets = self._gold.get("datasets", {}).get("facts", {})
        physical = datasets.get(logical_name, logical_name)
        return self._read(self._gold_root / self._gold["storage"]["facts_root"] / physical)

    def read_dimension(self, logical_name: str) -> pd.DataFrame:
        datasets = self._gold.get("datasets", {}).get("dimensions", {})
        physical = datasets.get(logical_name, logical_name)
        return self._read(self._gold_root / self._gold["storage"]["dimensions_root"] / physical)

    def read_feature(self, logical_name: str) -> pd.DataFrame:
        datasets = self._gold.get("datasets", {}).get("ml_features", {})
        physical = datasets.get(logical_name, logical_name)
        return self._read(self._gold_root / self._gold["storage"]["features_root"] / physical)

    def read_ml(self, dataset_name: str) -> pd.DataFrame:
        return self._read(self._ml_root / dataset_name)

    def ml_dataset_name(self, section: str, key: str) -> str:
        return str(self._ml.get(section, {}).get(key, key))

    def location_for_mart(self, logical_name: str) -> DatasetLocation:
        physical = self._gold.get("datasets", {}).get("marts", {}).get(logical_name, logical_name)
        path = self._gold_root / self._gold["storage"]["marts_root"] / physical
        return DatasetLocation(logical_name, physical, path)

    @staticmethod
    def _read(path: Path) -> pd.DataFrame:
        return _read_parquet_cached(str(path), _dataset_signature(path))


def normalize_service_names(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "service_type" not in frame.columns:
        return frame
    result = frame.copy()
    mapping = {
        "yellow": "Yellow",
        "green": "Green",
        "fhv": "FHV",
        "fhvhv": "HVFHV",
    }
    result["service_label"] = result["service_type"].astype(str).str.lower().map(mapping).fillna(
        result["service_type"].astype(str).str.upper()
    )
    return result


def numeric(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = frame.copy()
    for column in columns:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    return result
