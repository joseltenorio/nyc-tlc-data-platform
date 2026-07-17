from __future__ import annotations

import csv
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from tlc_data_platform.bronze.manifest import JsonEncoder
from tlc_data_platform.core.settings import AppConfig
from tlc_data_platform.silver.models import ReferenceRefreshSummary, utc_now
from tlc_data_platform.silver.spark import SilverSparkProvider
from tlc_data_platform.silver.storage import SilverStorage


@dataclass(frozen=True)
class DownloadedReference:
    path: Path
    sha256: str
    size_bytes: int
    source_url: str


class SilverReferencePipeline:
    """Lands immutable raw reference CSVs in Bronze and publishes curated Silver Parquet."""

    def __init__(
        self,
        config: AppConfig,
        *,
        session: requests.Session | None = None,
        spark_provider: SilverSparkProvider | None = None,
        storage: SilverStorage | None = None,
        close_shared_spark: bool = False,
    ) -> None:
        self._config = config
        self._session = session or requests.Session()
        self._owns_session = session is None
        self._spark = spark_provider or SilverSparkProvider(config.silver.spark)
        self._owns_spark = spark_provider is None or close_shared_spark
        self._storage = storage or SilverStorage(config.silver.storage)

    def close(self) -> None:
        if self._owns_session:
            self._session.close()
        if self._owns_spark:
            self._spark.close()

    def run(self) -> ReferenceRefreshSummary:
        self._storage.ensure_directories()
        spark = self._spark.get()
        refreshed_at = utc_now()
        execution_id = f"references-{refreshed_at.strftime('%Y%m%dT%H%M%S')}-{os.getpid()}"
        temp_root = self._config.silver.storage.temporary_root / execution_id
        temp_root.mkdir(parents=True, exist_ok=True)
        try:
            taxi_download = self._download(
                self._config.silver.references.taxi_zones_url,
                temp_root / "taxi_zone_lookup.csv",
            )
            base_download = self._download(
                self._config.silver.references.base_lookup_url,
                temp_root / "base_lookup.csv",
            )
            taxi_bronze = self._persist_bronze(
                "taxi_zones", "taxi_zone_lookup.csv", taxi_download
            )
            base_bronze = self._persist_bronze(
                "base_lookup", "base_lookup.csv", base_download
            )

            taxi_df = self._normalize_taxi_zones(spark, taxi_bronze, taxi_download.sha256)
            base_df = self._normalize_bases(spark, base_bronze, base_download.sha256)
            taxi_df = taxi_df.persist()
            base_df = base_df.persist()
            try:
                taxi_rows = taxi_df.count()
                base_rows = base_df.count()
                if taxi_rows < 260:
                    raise ValueError(
                        f"Taxi Zone Lookup quedó incompleto: {taxi_rows} filas normalizadas"
                    )
                if base_rows == 0:
                    raise ValueError("Current Bases no contiene filas normalizadas")

                taxi_temp = temp_root / "taxi_zones_parquet"
                base_temp = temp_root / "base_lookup_parquet"
                options = {
                    "compression": self._config.silver.execution.parquet_compression
                }
                taxi_df.write.mode("overwrite").options(**options).parquet(str(taxi_temp))
                base_df.write.mode("overwrite").options(**options).parquet(str(base_temp))
                taxi_final, base_final = self._storage.promote_references(
                    taxi_temp, base_temp
                )
            finally:
                taxi_df.unpersist()
                base_df.unpersist()

            manifest_path = self._write_manifest(
                execution_id=execution_id,
                refreshed_at=refreshed_at,
                taxi_download=taxi_download,
                base_download=base_download,
                taxi_bronze=taxi_bronze,
                base_bronze=base_bronze,
                taxi_final=taxi_final,
                base_final=base_final,
                taxi_rows=taxi_rows,
                base_rows=base_rows,
            )
            return ReferenceRefreshSummary(
                status="SUCCESS",
                taxi_zones_rows=taxi_rows,
                base_lookup_rows=base_rows,
                taxi_zones_path=str(taxi_final),
                base_lookup_path=str(base_final),
                taxi_zones_bronze_path=str(taxi_bronze),
                base_lookup_bronze_path=str(base_bronze),
                taxi_zones_sha256=taxi_download.sha256,
                base_lookup_sha256=base_download.sha256,
                manifest_path=str(manifest_path),
                refreshed_at=refreshed_at,
            )
        finally:
            import shutil

            if temp_root.exists():
                shutil.rmtree(temp_root)

    def _download(self, url: str, path: Path) -> DownloadedReference:
        response = self._session.get(
            url,
            timeout=self._config.silver.references.request_timeout_seconds,
            headers={"User-Agent": self._config.discovery.user_agent},
        )
        response.raise_for_status()
        content = response.content
        if len(content) < 10 or b"<html" in content[:500].lower():
            raise ValueError(f"La referencia no devolvió un CSV válido: {url}")
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".part")
        with temporary.open("wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            if not next(csv.reader(handle), None):
                raise ValueError(f"CSV sin cabecera: {url}")
        return DownloadedReference(
            path=path,
            sha256=hashlib.sha256(content).hexdigest(),
            size_bytes=len(content),
            source_url=url,
        )

    def _persist_bronze(
        self,
        dataset: str,
        file_name: str,
        downloaded: DownloadedReference,
    ) -> Path:
        target = (
            self._config.silver.references.bronze_root
            / dataset
            / f"sha256={downloaded.sha256}"
            / file_name
        )
        if target.is_file():
            return target
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + ".part")
        with temporary.open("wb") as handle:
            handle.write(downloaded.path.read_bytes())
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(target)
        return target

    def _write_manifest(
        self,
        *,
        execution_id: str,
        refreshed_at: Any,
        taxi_download: DownloadedReference,
        base_download: DownloadedReference,
        taxi_bronze: Path,
        base_bronze: Path,
        taxi_final: Path,
        base_final: Path,
        taxi_rows: int,
        base_rows: int,
    ) -> Path:
        path = self._config.silver.storage.manifests_root / f"{execution_id}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "execution_id": execution_id,
            "layer": "silver_reference",
            "status": "SUCCESS",
            "refreshed_at": refreshed_at,
            "references": [
                {
                    "name": "taxi_zones",
                    "source_url": taxi_download.source_url,
                    "sha256": taxi_download.sha256,
                    "size_bytes": taxi_download.size_bytes,
                    "bronze_path": str(taxi_bronze),
                    "silver_path": str(taxi_final),
                    "rows": taxi_rows,
                },
                {
                    "name": "base_lookup",
                    "source_url": base_download.source_url,
                    "sha256": base_download.sha256,
                    "size_bytes": base_download.size_bytes,
                    "bronze_path": str(base_bronze),
                    "silver_path": str(base_final),
                    "rows": base_rows,
                },
            ],
        }
        temp = path.with_suffix(".json.tmp")
        temp.write_text(
            json.dumps(payload, cls=JsonEncoder, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp.replace(path)
        return path

    @staticmethod
    def _find_column(df: Any, aliases: list[str]) -> str | None:
        lookup = {name.lower().replace(" ", "_"): name for name in df.columns}
        for alias in aliases:
            value = lookup.get(alias.lower().replace(" ", "_"))
            if value:
                return value
        return None

    def _normalize_taxi_zones(self, spark: Any, path: Path, source_sha256: str) -> Any:
        from pyspark.sql import functions as F

        raw = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(str(path))
        )
        location = self._find_column(raw, ["LocationID", "location_id"])
        borough = self._find_column(raw, ["Borough"])
        zone = self._find_column(raw, ["Zone"])
        service_zone = self._find_column(raw, ["service_zone"])
        if not all([location, borough, zone, service_zone]):
            raise ValueError(f"Esquema inesperado en Taxi Zone Lookup: {raw.columns}")
        return (
            raw.select(
                F.col(location).cast("int").alias("location_id"),
                F.trim(F.col(borough)).alias("borough"),
                F.trim(F.col(zone)).alias("zone_name"),
                F.trim(F.col(service_zone)).alias("service_zone"),
            )
            .filter(F.col("location_id").isNotNull())
            .dropDuplicates(["location_id"])
            .withColumn("is_airport", F.col("location_id").isin(1, 132, 138))
            .withColumn(
                "airport_name",
                F.when(F.col("location_id") == 1, F.lit("Newark Liberty International Airport"))
                .when(F.col("location_id") == 132, F.lit("John F. Kennedy International Airport"))
                .when(F.col("location_id") == 138, F.lit("LaGuardia Airport"))
                .otherwise(F.lit(None).cast("string")),
            )
            .withColumn("source_url", F.lit(self._config.silver.references.taxi_zones_url))
            .withColumn("source_sha256", F.lit(source_sha256))
            .withColumn("refreshed_at", F.current_timestamp())
        )

    def _normalize_bases(self, spark: Any, path: Path, source_sha256: str) -> Any:
        from pyspark.sql import functions as F

        raw = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(str(path))
        )
        license_col = self._find_column(
            raw,
            ["License Number", "license_number", "base_license_number", "base_license"],
        )
        name_col = self._find_column(raw, ["Base Name", "base_name", "name"])
        type_col = self._find_column(
            raw, ["Type", "base_type", "license_type", "base_license_type"]
        )
        status_col = self._find_column(raw, ["Status", "base_status"])
        dba_col = self._find_column(raw, ["DBA", "doing_business_as"])
        phone_col = self._find_column(
            raw, ["Base Telephone Number", "base_telephone_number", "telephone"]
        )
        if not license_col:
            raise ValueError(f"Esquema inesperado en Current Bases: {raw.columns}")
        return (
            raw.select(
                F.upper(F.trim(F.col(license_col))).alias("base_license_number"),
                (
                    F.trim(F.col(name_col))
                    if name_col
                    else F.lit(None).cast("string")
                ).alias("base_name"),
                (
                    F.trim(F.col(dba_col))
                    if dba_col
                    else F.lit(None).cast("string")
                ).alias("doing_business_as"),
                (
                    F.trim(F.col(type_col))
                    if type_col
                    else F.lit(None).cast("string")
                ).alias("base_type"),
                (
                    F.trim(F.col(status_col))
                    if status_col
                    else F.lit(None).cast("string")
                ).alias("status"),
                (
                    F.trim(F.col(phone_col))
                    if phone_col
                    else F.lit(None).cast("string")
                ).alias("telephone"),
            )
            .filter(
                F.col("base_license_number").isNotNull()
                & (F.length("base_license_number") > 0)
            )
            .dropDuplicates(["base_license_number"])
            .withColumn("source_url", F.lit(self._config.silver.references.base_lookup_url))
            .withColumn("source_sha256", F.lit(source_sha256))
            .withColumn("refreshed_at", F.current_timestamp())
        )
