from __future__ import annotations

from pathlib import Path

import pytest

from tlc_data_platform.bronze.models import FileCandidate
from tlc_data_platform.core.exceptions import ParquetValidationError
from tlc_data_platform.ingestion.parquet_validator import (
    ParquetValidator,
    match_contract_fields,
    normalize_spark_type_name,
)


class FakeType:
    def __init__(self, value):
        self.value = value

    def simpleString(self):
        return self.value


class FakeField:
    def __init__(self, name, data_type):
        self.name = name
        self.dataType = FakeType(data_type)


class FakeSchema:
    def __init__(self, fields):
        self.fields = [FakeField(name, dtype) for name, dtype in fields]

    def fieldNames(self):
        return [field.name for field in self.fields]

    def json(self):
        return '{"fields":[' + ','.join(f'"{f.name}:{f.dataType.value}"' for f in self.fields) + ']}'


class FakeDF:
    def __init__(self, fields):
        self.schema = FakeSchema(fields)

    def limit(self, count):
        return self

    def collect(self):
        return [object()]


class FakeRead:
    def __init__(self, fields):
        self.fields = fields

    def parquet(self, path):
        return FakeDF(self.fields)


class FakeSpark:
    def __init__(self, fields):
        self.read = FakeRead(fields)


def write_yellow_parquet(path: Path, include_new=False):
    import pyarrow as pa
    import pyarrow.parquet as pq

    data = {
        "tpep_pickup_datetime": pa.array(["2026-01-01T00:00:00"], type=pa.string()),
        "tpep_dropoff_datetime": pa.array(["2026-01-01T00:10:00"], type=pa.string()),
        "PULocationID": pa.array([1], type=pa.int64()),
        "DOLocationID": pa.array([2], type=pa.int64()),
    }
    if include_new:
        data["future_fee"] = pa.array([1.5], type=pa.float64())
    pq.write_table(pa.table(data), path)


def yellow_candidate():
    return FileCandidate(
        service="yellow",
        year=2026,
        month=1,
        url="https://example/yellow_tripdata_2026-01.parquet",
        file_name="yellow_tripdata_2026-01.parquet",
        discovery_method="html",
    )


def fhv_candidate():
    return FileCandidate(
        service="fhv",
        year=2019,
        month=1,
        url="https://example/fhv_tripdata_2019-01.parquet",
        file_name="fhv_tripdata_2019-01.parquet",
        discovery_method="html",
    )


def valid_fields(include_new=False):
    fields = [
        ("tpep_pickup_datetime", "timestamp"),
        ("tpep_dropoff_datetime", "timestamp"),
        ("PULocationID", "long"),
        ("DOLocationID", "long"),
    ]
    if include_new:
        fields.append(("future_fee", "double"))
    return fields


def test_alias_matching_is_case_insensitive():
    matches, missing = match_contract_fields(
        ["PULocationID", "dropOff_datetime"],
        {
            "pickup": {"aliases": ["pulocationid"]},
            "dropoff": {"aliases": ["dropoff_datetime"]},
        },
    )
    assert matches == {"pickup": "PULocationID", "dropoff": "dropOff_datetime"}
    assert missing == []


def test_normalize_spark_type_name_handles_known_aliases():
    assert normalize_spark_type_name("int") == "integer"
    assert normalize_spark_type_name("integer") == "integer"
    assert normalize_spark_type_name("bigint") == "long"
    assert normalize_spark_type_name("long") == "long"
    assert normalize_spark_type_name("decimal(10,2)") == "decimal(10,2)"


def test_rejects_invalid_signature(tmp_path, app_config):
    path = tmp_path / "yellow_tripdata_2026-01.parquet"
    path.write_bytes(b"not parquet")
    validator = ParquetValidator(app_config.schema_contracts, app_config.validation)
    with pytest.raises(ParquetValidationError, match="Firma Parquet"):
        validator.validate(path, yellow_candidate(), FakeSpark(valid_fields()))


def test_valid_file_returns_physical_metadata(tmp_path, app_config):
    path = tmp_path / "yellow_tripdata_2026-01.parquet"
    write_yellow_parquet(path)
    validator = ParquetValidator(app_config.schema_contracts, app_config.validation)
    result = validator.validate(path, yellow_candidate(), FakeSpark(valid_fields()))
    assert result.parquet_num_rows == 1
    assert result.parquet_num_row_groups >= 1
    assert result.parquet_num_columns == 4
    assert result.sample_rows_read == 1
    assert result.schema_hash


def test_missing_required_column_fails(tmp_path, app_config):
    path = tmp_path / "yellow_tripdata_2026-01.parquet"
    write_yellow_parquet(path)
    fields = valid_fields()[:-1]
    validator = ParquetValidator(app_config.schema_contracts, app_config.validation)
    with pytest.raises(ParquetValidationError, match="Faltan campos requeridos"):
        validator.validate(path, yellow_candidate(), FakeSpark(fields))


def test_new_column_is_detected_without_rejection(tmp_path, app_config):
    path = tmp_path / "yellow_tripdata_2026-01.parquet"
    write_yellow_parquet(path, include_new=True)
    validator = ParquetValidator(app_config.schema_contracts, app_config.validation)
    result = validator.validate(path, yellow_candidate(), FakeSpark(valid_fields(True)))
    assert result.schema_evolution_detected is True
    assert result.new_columns == ["future_fee"]
    assert result.schema_events == ["SCHEMA_EVOLUTION_DETECTED"]


def test_timestamp_ntz_and_integer_aliases_are_accepted(tmp_path, app_config):
    path = tmp_path / "yellow_tripdata_2026-01.parquet"
    write_yellow_parquet(path)
    validator = ParquetValidator(app_config.schema_contracts, app_config.validation)
    result = validator.validate(
        path,
        yellow_candidate(),
        FakeSpark(
            [
                ("tpep_pickup_datetime", "timestamp_ntz"),
                ("tpep_dropoff_datetime", "timestamp_ntz"),
                ("PULocationID", "int"),
                ("DOLocationID", "integer"),
            ]
        ),
    )
    assert result.type_mismatches == {}


def test_bigint_alias_is_accepted_for_integer_contract(tmp_path, app_config):
    path = tmp_path / "yellow_tripdata_2026-01.parquet"
    write_yellow_parquet(path)
    validator = ParquetValidator(app_config.schema_contracts, app_config.validation)
    result = validator.validate(
        path,
        yellow_candidate(),
        FakeSpark(
            [
                ("tpep_pickup_datetime", "timestamp"),
                ("tpep_dropoff_datetime", "timestamp"),
                ("PULocationID", "bigint"),
                ("DOLocationID", "long"),
            ]
        ),
    )
    assert result.type_mismatches == {}


def test_fhv_historical_double_location_ids_are_accepted(tmp_path, app_config):
    path = tmp_path / "fhv_tripdata_2019-01.parquet"
    write_yellow_parquet(path)
    validator = ParquetValidator(app_config.schema_contracts, app_config.validation)
    result = validator.validate(
        path,
        fhv_candidate(),
        FakeSpark(
            [
                ("dispatching_base_num", "string"),
                ("pickup_datetime", "timestamp_ntz"),
                ("dropOff_datetime", "timestamp_ntz"),
                ("PUlocationID", "double"),
                ("DOlocationID", "double"),
            ]
        ),
    )
    assert result.required_field_matches["pickup_location_id"] == "PUlocationID"
    assert result.type_mismatches == {}


def test_filename_period_mismatch_fails(tmp_path, app_config):
    path = tmp_path / "yellow_tripdata_2026-01.parquet"
    write_yellow_parquet(path)
    candidate = FileCandidate(
        service="yellow",
        year=2026,
        month=2,
        url="x",
        file_name="yellow_tripdata_2026-01.parquet",
        discovery_method="html",
    )
    validator = ParquetValidator(app_config.schema_contracts, app_config.validation)
    with pytest.raises(ParquetValidationError, match="año y mes"):
        validator.validate(path, candidate, FakeSpark(valid_fields()))