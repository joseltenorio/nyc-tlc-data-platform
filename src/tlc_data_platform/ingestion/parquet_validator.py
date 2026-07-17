from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from tlc_data_platform.bronze.models import FileCandidate, ValidationResult
from tlc_data_platform.core.exceptions import ParquetValidationError
from tlc_data_platform.core.settings import ValidationConfig
from tlc_data_platform.ingestion.downloader import has_parquet_signature

FILE_PATTERN = re.compile(
    r"(?P<service>yellow|green|fhv|fhvhv)_tripdata_"
    r"(?P<year>\d{4})-(?P<month>\d{2})\.parquet$",
    re.IGNORECASE,
)


def _normalized_lookup(columns: list[str]) -> dict[str, str]:
    return {column.casefold(): column for column in columns}


def match_contract_fields(
    columns: list[str],
    definitions: dict[str, dict[str, Any]],
) -> tuple[dict[str, str], list[str]]:
    lookup = _normalized_lookup(columns)
    matches: dict[str, str] = {}
    missing: list[str] = []
    for logical_name, definition in definitions.items():
        aliases = [str(value) for value in definition.get("aliases", [])]
        match = next((lookup[a.casefold()] for a in aliases if a.casefold() in lookup), None)
        if match is None:
            missing.append(logical_name)
        else:
            matches[logical_name] = match
    return matches, missing


def _validate_candidate_name(candidate: FileCandidate) -> None:
    match = FILE_PATTERN.fullmatch(candidate.file_name)
    if not match:
        raise ParquetValidationError(
            f"Nombre de archivo no reconocido: {candidate.file_name}"
        )
    if match.group("service").lower() != candidate.service:
        raise ParquetValidationError("El nombre no corresponde al servicio esperado")
    if int(match.group("year")) != candidate.year or int(match.group("month")) != candidate.month:
        raise ParquetValidationError("El nombre no corresponde al año y mes esperados")


def _physical_metadata(path: Path) -> tuple[int, int, int, str | None, list[str]]:
    try:
        import pyarrow.parquet as pq

        parquet = pq.ParquetFile(path)
        metadata = parquet.metadata
        codecs: set[str] = set()
        for row_group_index in range(metadata.num_row_groups):
            row_group = metadata.row_group(row_group_index)
            for column_index in range(row_group.num_columns):
                codecs.add(str(row_group.column(column_index).compression))
        return (
            metadata.num_rows,
            metadata.num_row_groups,
            metadata.num_columns,
            metadata.created_by,
            sorted(codecs),
        )
    except Exception as exc:
        raise ParquetValidationError(f"No se pudo leer metadata Parquet: {exc}") from exc


class ParquetValidator:
    def __init__(
        self,
        contracts: dict[str, Any],
        config: ValidationConfig,
    ) -> None:
        self._contracts = contracts
        self._config = config

    def validate(self, path: Path, candidate: FileCandidate, spark: Any) -> ValidationResult:
        if not path.is_file() or path.stat().st_size == 0:
            raise ParquetValidationError("El archivo no existe o está vacío")
        if not has_parquet_signature(path):
            raise ParquetValidationError("Firma Parquet inválida")
        _validate_candidate_name(candidate)

        num_rows, num_row_groups, num_columns, created_by, codecs = _physical_metadata(path)
        if num_rows <= 0 or num_columns <= 0:
            raise ParquetValidationError("El Parquet no contiene filas o columnas")

        try:
            dataframe = spark.read.parquet(str(path))
            schema = dataframe.schema
            sample_rows_read = len(dataframe.limit(1).collect()) if self._config.read_sample_row else 0
        except Exception as exc:
            raise ParquetValidationError(f"PySpark no pudo leer el archivo: {exc}") from exc

        observed_columns = list(schema.fieldNames())
        observed_types = {
            field.name: field.dataType.simpleString().lower() for field in schema.fields
        }
        schema_json = schema.json()
        schema_hash = hashlib.sha256(schema_json.encode("utf-8")).hexdigest()

        contract = self._contracts[candidate.service]
        required_definitions = contract.get("required_fields", {})
        optional_definitions = contract.get("optional_fields", {})
        required_matches, missing_required = match_contract_fields(
            observed_columns, required_definitions
        )
        optional_matches, missing_optional = match_contract_fields(
            observed_columns, optional_definitions
        )
        if missing_required:
            raise ParquetValidationError(
                "Faltan campos requeridos: " + ", ".join(sorted(missing_required))
            )

        known_aliases = {
            str(alias).casefold()
            for definitions in (required_definitions, optional_definitions)
            for definition in definitions.values()
            for alias in definition.get("aliases", [])
        }
        new_columns = sorted(
            column for column in observed_columns if column.casefold() not in known_aliases
        )
        if new_columns and not self._config.allow_new_columns:
            raise ParquetValidationError(
                "Se detectaron columnas nuevas no permitidas: " + ", ".join(new_columns)
            )

        type_mismatches: dict[str, dict[str, Any]] = {}
        for logical_name, physical_name in required_matches.items():
            accepted = [
                str(value).lower()
                for value in required_definitions[logical_name].get("accepted_types", [])
            ]
            observed = observed_types[physical_name]
            if accepted and not any(observed == item or observed.startswith(f"{item}(") for item in accepted):
                type_mismatches[logical_name] = {
                    "column": physical_name,
                    "observed": observed,
                    "accepted": accepted,
                }
        if type_mismatches:
            raise ParquetValidationError(
                "Tipos incompatibles: " + json.dumps(type_mismatches, ensure_ascii=False)
            )

        return ValidationResult(
            expected_required_columns=sorted(required_definitions),
            expected_optional_columns=sorted(optional_definitions),
            observed_columns=observed_columns,
            missing_required_columns=[],
            missing_optional_columns=sorted(missing_optional),
            new_columns=new_columns,
            observed_types=observed_types,
            required_field_matches=required_matches,
            optional_field_matches=optional_matches,
            type_mismatches={},
            schema_json=schema_json,
            schema_hash=schema_hash,
            schema_evolution_detected=bool(new_columns),
            schema_events=["SCHEMA_EVOLUTION_DETECTED"] if new_columns else [],
            parquet_num_rows=num_rows,
            parquet_num_row_groups=num_row_groups,
            parquet_num_columns=num_columns,
            parquet_created_by=created_by,
            parquet_compression_codecs=codecs,
            sample_rows_read=sample_rows_read,
        )
