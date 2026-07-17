from __future__ import annotations

from typing import Any, Callable

from tlc_data_platform.silver.transformers import fhv, fhvhv, green, yellow

TRANSFORMERS: dict[str, Callable[..., Any]] = {
    "yellow": yellow.transform,
    "green": green.transform,
    "fhv": fhv.transform,
    "fhvhv": fhvhv.transform,
}


def get_transformer(service: str) -> Callable[..., Any]:
    try:
        return TRANSFORMERS[service]
    except KeyError as exc:
        raise ValueError(f"No existe transformador Silver para {service}") from exc
