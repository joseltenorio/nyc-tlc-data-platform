from __future__ import annotations

from tlc_data_platform.bronze.models import ExpectedPeriod
from tlc_data_platform.core.settings import AppConfig


def is_period_applicable(
    config: AppConfig, service: str, year: int, month: int
) -> bool:
    """Returns whether a period belongs to both the official and project scope.

    `available_from` describes TLC's official history. `scope_from/scope_to`
    describes the bounded dataset selected for this project. This prevents a
    generic historical or incremental command from downloading years/services
    intentionally excluded from the analytical coverage.
    """
    service_config = config.services[service]
    point = (year, month)
    return (
        point >= (service_config.available_from.year, service_config.available_from.month)
        and point >= (service_config.scope_from.year, service_config.scope_from.month)
        and point <= (service_config.scope_to.year, service_config.scope_to.month)
    )


def generate_expected_periods(
    config: AppConfig,
    services: list[str],
    start_year: int,
    end_year: int,
    months: list[int],
) -> list[ExpectedPeriod]:
    periods: list[ExpectedPeriod] = []
    for service in sorted(services):
        for year in range(start_year, end_year + 1):
            for month in sorted(set(months)):
                periods.append(
                    ExpectedPeriod(
                        service=service,
                        year=year,
                        month=month,
                        applicable=is_period_applicable(
                            config, service, year, month
                        ),
                    )
                )
    return periods
