from __future__ import annotations

from tlc_data_platform.bronze.models import ExpectedPeriod
from tlc_data_platform.core.settings import AppConfig


def generate_expected_periods(
    config: AppConfig,
    services: list[str],
    start_year: int,
    end_year: int,
    months: list[int],
) -> list[ExpectedPeriod]:
    periods: list[ExpectedPeriod] = []
    for service in sorted(services):
        available_from = config.services[service].available_from
        for year in range(start_year, end_year + 1):
            for month in sorted(set(months)):
                applicable = (year, month) >= (
                    available_from.year,
                    available_from.month,
                )
                periods.append(
                    ExpectedPeriod(
                        service=service,
                        year=year,
                        month=month,
                        applicable=applicable,
                    )
                )
    return periods
