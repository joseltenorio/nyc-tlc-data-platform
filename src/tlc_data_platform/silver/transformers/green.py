from __future__ import annotations

from typing import Any

from tlc_data_platform.core.settings import SilverQualityConfig
from tlc_data_platform.silver.models import SilverTransformContext
from tlc_data_platform.silver.transformers.taxi import transform_taxi


def transform(raw: Any, context: SilverTransformContext, quality: SilverQualityConfig) -> Any:
    return transform_taxi(
        raw,
        context,
        quality,
        pickup_alias="lpep_pickup_datetime",
        dropoff_alias="lpep_dropoff_datetime",
        vendor_values=(1, 2, 6),
        include_trip_type=True,
        include_ehail_fee=True,
    )
