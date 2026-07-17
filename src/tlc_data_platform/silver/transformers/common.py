from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from tlc_data_platform.core.settings import SilverQualityConfig
from tlc_data_platform.silver.models import SilverTransformContext


@dataclass(frozen=True)
class Rule:
    code: str
    severity: str
    condition: Any


def _column_lookup(df: Any) -> dict[str, str]:
    return {name.lower(): name for name in df.columns}


def col_from_aliases(df: Any, aliases: Iterable[str], dtype: str, default: Any = None) -> Any:
    from pyspark.sql import functions as F

    lookup = _column_lookup(df)
    for alias in aliases:
        actual = lookup.get(alias.lower())
        if actual is not None:
            return F.col(actual).cast(dtype)
    return F.lit(default).cast(dtype)


def normalize_string(column: Any) -> Any:
    from pyspark.sql import functions as F

    return F.upper(F.trim(column.cast("string")))


def normalize_yn(column: Any) -> Any:
    from pyspark.sql import functions as F

    value = normalize_string(column)
    return F.when(value.isin("Y", "N"), value).otherwise(F.lit(None).cast("string"))


def normalize_boolean(column: Any) -> Any:
    from pyspark.sql import functions as F

    value = normalize_string(column)
    return (
        F.when(value.isin("Y", "YES", "TRUE", "1"), F.lit(True))
        .when(value.isin("N", "NO", "FALSE", "0"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def add_common_metadata(df: Any, context: SilverTransformContext) -> Any:
    from pyspark.sql import functions as F

    return (
        df.withColumn("service_type", F.lit(context.service))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("dropoff_date", F.to_date("dropoff_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime").cast("int"))
        .withColumn("pickup_day_of_week", F.dayofweek("pickup_datetime").cast("int"))
        .withColumn("weekend_trip_flag", F.dayofweek("pickup_datetime").isin(1, 7))
        .withColumn(
            "night_trip_flag",
            (F.hour("pickup_datetime") < 6) | (F.hour("pickup_datetime") >= 22),
        )
        .withColumn("source_file", F.lit(context.source_file))
        .withColumn("source_year", F.lit(context.year).cast("int"))
        .withColumn("source_month", F.lit(context.month).cast("int"))
        .withColumn("bronze_sha256", F.lit(context.source_sha256).cast("string"))
        .withColumn("bronze_execution_id", F.lit(context.bronze_execution_id).cast("string"))
        .withColumn("silver_execution_id", F.lit(context.silver_execution_id))
        .withColumn("silver_processed_at", F.current_timestamp())
    )


def add_trip_business_id(df: Any, fields: list[str]) -> Any:
    from pyspark.sql import functions as F

    values = [F.coalesce(F.col(name).cast("string"), F.lit("∅")) for name in fields]
    return df.withColumn("trip_business_id", F.sha2(F.concat_ws("||", *values), 256))


def common_rules(df: Any, context: SilverTransformContext, quality: SilverQualityConfig, max_duration_hours: float) -> list[Rule]:
    from pyspark.sql import functions as F

    duration = F.col("trip_duration_seconds")
    pickup = F.col("pickup_datetime")
    return [
        Rule("MISSING_PICKUP_DATETIME", "ERROR", pickup.isNull()),
        Rule("MISSING_DROPOFF_DATETIME", "ERROR", F.col("dropoff_datetime").isNull()),
        Rule("INVALID_DATE_ORDER", "ERROR", duration <= 0),
        Rule("DURATION_ABOVE_LIMIT", "ERROR", duration > int(max_duration_hours * 3600)),
        Rule(
            "PICKUP_OUTSIDE_SOURCE_PERIOD",
            "ERROR",
            pickup.isNotNull()
            & ((F.year(pickup) != F.lit(context.year)) | (F.month(pickup) != F.lit(context.month))),
        ),
        Rule(
            "INVALID_PICKUP_LOCATION",
            "ERROR",
            F.col("pickup_location_id").isNull()
            | ~F.col("pickup_location_id").between(quality.valid_location_id_min, quality.valid_location_id_max),
        ),
        Rule(
            "INVALID_DROPOFF_LOCATION",
            "ERROR",
            F.col("dropoff_location_id").isNull()
            | ~F.col("dropoff_location_id").between(quality.valid_location_id_min, quality.valid_location_id_max),
        ),
    ]


def apply_rules(df: Any, rules: list[Rule], deduplicate: bool = True) -> Any:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if deduplicate:
        window = Window.partitionBy("trip_business_id").orderBy(F.monotonically_increasing_id())
        df = df.withColumn("_duplicate_rank", F.row_number().over(window))
        rules = [*rules, Rule("DUPLICATE_TRIP", "ERROR", F.col("_duplicate_rank") > 1)]

    error_exprs = [F.when(rule.condition, F.lit(rule.code)) for rule in rules if rule.severity == "ERROR"]
    warning_exprs = [F.when(rule.condition, F.lit(rule.code)) for rule in rules if rule.severity == "WARNING"]
    empty = F.array().cast("array<string>")
    error_array = F.filter(F.array(*error_exprs), lambda item: item.isNotNull()) if error_exprs else empty
    warning_array = F.filter(F.array(*warning_exprs), lambda item: item.isNotNull()) if warning_exprs else empty
    df = (
        df.withColumn("quality_error_codes", error_array)
        .withColumn("quality_warning_codes", warning_array)
        .withColumn("quality_error_count", F.size("quality_error_codes"))
        .withColumn("quality_warning_count", F.size("quality_warning_codes"))
        .withColumn(
            "quality_status",
            F.when(F.col("quality_error_count") > 0, F.lit("REJECTED"))
            .when(F.col("quality_warning_count") > 0, F.lit("WARNING"))
            .otherwise(F.lit("VALID")),
        )
    )
    return df.drop("_duplicate_rank") if "_duplicate_rank" in df.columns else df


def split_valid_rejected(df: Any) -> tuple[Any, Any]:
    from pyspark.sql import functions as F

    return (
        df.filter(F.col("quality_error_count") == 0),
        df.filter(F.col("quality_error_count") > 0),
    )


def taxi_rules(df: Any, context: SilverTransformContext, quality: SilverQualityConfig) -> list[Rule]:
    from pyspark.sql import functions as F

    rules = common_rules(df, context, quality, quality.taxi_max_duration_hours)
    rules.extend(
        [
            Rule("PASSENGER_COUNT_OUT_OF_RANGE", "ERROR", (F.col("passenger_count") < 0) | (F.col("passenger_count") > quality.max_passenger_count)),
            Rule("PASSENGER_COUNT_IMPUTED", "WARNING", F.col("passenger_count_imputed")),
            Rule("INVALID_TRIP_DISTANCE", "ERROR", F.col("trip_distance").isNull() | (F.col("trip_distance") < 0) | (F.col("trip_distance") > quality.max_trip_distance_miles)),
            Rule("ZERO_TRIP_DISTANCE", "ERROR" if quality.reject_zero_distance else "WARNING", F.col("trip_distance") == 0),
            Rule("NEGATIVE_FARE_AMOUNT", "ERROR", F.col("fare_amount").isNull() | (F.col("fare_amount") < 0)),
            Rule("NON_POSITIVE_TOTAL_AMOUNT", "ERROR", F.col("total_amount").isNull() | (F.col("total_amount") <= 0)),
            Rule("TOTAL_AMOUNT_ABOVE_LIMIT", "ERROR", F.col("total_amount") > quality.max_total_amount),
            Rule("TIP_EXCEEDS_FARE", "WARNING", (F.col("tip_amount") > 50) & (F.col("tip_amount") > F.col("fare_amount"))),
            Rule("SUSPICIOUS_TOLL_AMOUNT", "WARNING", (F.col("tolls_amount") > 40) & (F.col("trip_distance") < 5)),
            Rule("INVALID_VENDOR_NORMALIZED", "WARNING", F.col("vendor_normalized")),
            Rule("INVALID_RATE_CODE_NORMALIZED", "WARNING", F.col("rate_code_normalized")),
            Rule("INVALID_PAYMENT_TYPE_NORMALIZED", "WARNING", F.col("payment_type_normalized")),
            Rule("INVALID_STORE_FLAG_NORMALIZED", "WARNING", F.col("store_flag_normalized")),
        ]
    )
    if quality.reject_negative_component_amounts:
        components = ["extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "congestion_surcharge", "airport_fee", "cbd_congestion_fee"]
        rules.append(Rule("NEGATIVE_COMPONENT_AMOUNT", "ERROR", F.greatest(*[F.when(F.col(c) < 0, F.lit(1)).otherwise(F.lit(0)) for c in components]) == 1))
    return rules


def append_quality_rules(
    df: Any,
    *,
    error_conditions: dict[str, Any] | None = None,
    warning_conditions: dict[str, Any] | None = None,
) -> Any:
    """Appends post-transformation quality rules and recalculates the row status."""
    from pyspark.sql import functions as F

    error_conditions = error_conditions or {}
    warning_conditions = warning_conditions or {}
    error_items = [
        F.when(condition, F.lit(code)) for code, condition in error_conditions.items()
    ]
    warning_items = [
        F.when(condition, F.lit(code)) for code, condition in warning_conditions.items()
    ]
    empty = F.array().cast("array<string>")
    new_errors = (
        F.filter(F.array(*error_items), lambda item: item.isNotNull())
        if error_items
        else empty
    )
    new_warnings = (
        F.filter(F.array(*warning_items), lambda item: item.isNotNull())
        if warning_items
        else empty
    )
    return (
        df.withColumn(
            "quality_error_codes",
            F.array_distinct(F.concat(F.col("quality_error_codes"), new_errors)),
        )
        .withColumn(
            "quality_warning_codes",
            F.array_distinct(F.concat(F.col("quality_warning_codes"), new_warnings)),
        )
        .withColumn("quality_error_count", F.size("quality_error_codes"))
        .withColumn("quality_warning_count", F.size("quality_warning_codes"))
        .withColumn(
            "quality_status",
            F.when(F.col("quality_error_count") > 0, F.lit("REJECTED"))
            .when(F.col("quality_warning_count") > 0, F.lit("WARNING"))
            .otherwise(F.lit("VALID")),
        )
    )
