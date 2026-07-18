from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class ParquetMetrics:
    parquet_files: int
    bytes_on_disk: int
    rows: int | None

    def to_dict(self) -> dict[str, int | None]:
        return asdict(self)


def parquet_files(path: Path) -> list[Path]:
    if path.is_file() and path.suffix.lower() == ".parquet":
        return [path]
    if not path.is_dir():
        return []
    return sorted(item for item in path.rglob("*.parquet") if item.is_file())


def directory_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    if not path.is_dir():
        return 0
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            try:
                total += item.stat().st_size
            except OSError:
                continue
    return total


def parquet_metrics(path: Path, *, include_rows: bool = True) -> ParquetMetrics:
    files = parquet_files(path)
    total_bytes = sum(item.stat().st_size for item in files)
    if not include_rows or not files:
        return ParquetMetrics(len(files), total_bytes, 0 if not files else None)

    rows = 0
    try:
        import pyarrow.parquet as pq

        for item in files:
            metadata = pq.ParquetFile(item).metadata
            rows += int(metadata.num_rows)
    except Exception:
        return ParquetMetrics(len(files), total_bytes, None)
    return ParquetMetrics(len(files), total_bytes, rows)


def aggregate_metrics(paths: Iterable[Path], *, include_rows: bool = True) -> ParquetMetrics:
    file_count = 0
    total_bytes = 0
    total_rows = 0
    rows_known = True
    for path in paths:
        current = parquet_metrics(path, include_rows=include_rows)
        file_count += current.parquet_files
        total_bytes += current.bytes_on_disk
        if current.rows is None:
            rows_known = False
        else:
            total_rows += current.rows
    return ParquetMetrics(file_count, total_bytes, total_rows if rows_known else None)
