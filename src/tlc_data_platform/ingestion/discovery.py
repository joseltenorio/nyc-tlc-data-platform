from __future__ import annotations

import logging
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from tlc_data_platform.bronze.models import (
    AvailabilityRecord,
    DiscoveryResult,
    FileCandidate,
)
from tlc_data_platform.core.settings import AppConfig
from tlc_data_platform.ingestion.expected_periods import generate_expected_periods
from tlc_data_platform.ingestion.http_client import HttpClient
from tlc_data_platform.ingestion.remote_probe import RemoteProbe

LOGGER = logging.getLogger(__name__)
FILE_PATTERN = re.compile(
    r"(?P<prefix>yellow|green|fhv|fhvhv)_tripdata_"
    r"(?P<year>\d{4})-(?P<month>\d{2})\.parquet$",
    re.IGNORECASE,
)


class FileDiscovery:
    """HTML-first discovery completed by deterministic probes for missing periods."""

    def __init__(
        self,
        config: AppConfig,
        http: HttpClient,
        probe: RemoteProbe,
    ) -> None:
        self._config = config
        self._http = http
        self._probe = probe

    def discover(
        self,
        execution_id: str,
        services: list[str],
        start_year: int,
        end_year: int,
        months: list[int],
    ) -> DiscoveryResult:
        if self._config.discovery.strategy != "html_with_deterministic_fallback":
            raise ValueError(
                f"Estrategia de descubrimiento no soportada: {self._config.discovery.strategy}"
            )
        expected = generate_expected_periods(
            self._config, services, start_year, end_year, months
        )
        expected_map = {(p.service, p.year, p.month): p for p in expected}
        candidates: dict[tuple[str, int, int], FileCandidate] = {}
        html_error: str | None = None

        try:
            html = self._http.get_text(self._config.source.landing_page)
            for candidate in self._parse_html(html):
                key = (candidate.service, candidate.year, candidate.month)
                period = expected_map.get(key)
                if period is not None and period.applicable:
                    candidates[key] = candidate
        except Exception as exc:
            html_error = str(exc)
            LOGGER.warning("No se pudo descubrir desde HTML: %s", exc)

        availability: list[AvailabilityRecord] = []
        for period in expected:
            key = (period.service, period.year, period.month)
            if not period.applicable:
                availability.append(
                    AvailabilityRecord(
                        execution_id=execution_id,
                        service=period.service,
                        year=period.year,
                        month=period.month,
                        status="NOT_APPLICABLE",
                        applicable=False,
                        expected=True,
                    )
                )
                continue

            if key in candidates:
                candidate = candidates[key]
                availability.append(
                    AvailabilityRecord(
                        execution_id=execution_id,
                        service=period.service,
                        year=period.year,
                        month=period.month,
                        status="DISCOVERED",
                        applicable=True,
                        expected=True,
                        candidate_url=candidate.url,
                        discovery_method="html",
                    )
                )
                continue

            candidate = self._deterministic_candidate(period.service, period.year, period.month)
            remote = self._probe.probe(candidate.url)
            if remote.available:
                candidates[key] = candidate
                status = "AVAILABLE"
            elif remote.probe_failed:
                status = "FAILED_TO_PROBE"
            else:
                status = "NOT_PUBLISHED_YET"
            availability.append(
                AvailabilityRecord(
                    execution_id=execution_id,
                    service=period.service,
                    year=period.year,
                    month=period.month,
                    status=status,
                    applicable=True,
                    expected=True,
                    candidate_url=candidate.url,
                    discovery_method="deterministic_fallback",
                    remote_metadata=remote,
                )
            )

        return DiscoveryResult(
            expected_periods=expected,
            candidates=sorted(candidates.values()),
            availability=availability,
            html_error=html_error,
        )

    def _parse_html(self, html: str) -> list[FileCandidate]:
        soup = BeautifulSoup(html, "html.parser")
        found: dict[tuple[str, int, int], FileCandidate] = {}
        for anchor in soup.find_all("a", href=True):
            url = urljoin(self._config.source.landing_page, anchor["href"].strip())
            parsed = urlparse(url)
            if parsed.hostname is None or parsed.hostname.lower() not in self._config.source.allowed_hosts:
                continue
            file_name = parsed.path.rsplit("/", 1)[-1]
            match = FILE_PATTERN.search(file_name)
            if not match:
                continue
            service = match.group("prefix").lower()
            year = int(match.group("year"))
            month = int(match.group("month"))
            found[(service, year, month)] = FileCandidate(
                service=service,
                year=year,
                month=month,
                url=url,
                file_name=file_name,
                discovery_method="html",
            )
        return sorted(found.values())

    def _deterministic_candidate(self, service: str, year: int, month: int) -> FileCandidate:
        prefix = self._config.services[service].file_prefix
        file_name = f"{prefix}_{year}-{month:02d}.parquet"
        return FileCandidate(
            service=service,
            year=year,
            month=month,
            url=f"{self._config.source.parquet_base_url}/{file_name}",
            file_name=file_name,
            discovery_method="deterministic_fallback",
        )
