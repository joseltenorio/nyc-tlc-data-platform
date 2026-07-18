from __future__ import annotations

import email.utils
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

import requests

from tlc_data_platform.core.settings import DiscoveryConfig, DownloadConfig

LOGGER = logging.getLogger(__name__)


class HttpClient:
    """Thread-local HTTP sessions with bounded retries and Retry-After support."""

    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(
        self,
        discovery: DiscoveryConfig,
        download: DownloadConfig,
    ) -> None:
        self._discovery = discovery
        self._download = download
        self._local = threading.local()
        self._sessions: list[requests.Session] = []
        self._sessions_lock = threading.Lock()

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({"User-Agent": self._discovery.user_agent})
            self._local.session = session
            with self._sessions_lock:
                self._sessions.append(session)
        return session

    @property
    def timeout(self) -> tuple[int, int]:
        return (
            self._download.connect_timeout_seconds,
            self._download.read_timeout_seconds,
        )

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        max_retries = int(kwargs.pop("max_retries_override", self._download.max_retries))
        if max_retries < 0:
            raise ValueError("max_retries_override no puede ser negativo")
        max_attempts = max_retries + 1
        backoff = self._download.initial_backoff_seconds
        last_error: Exception | None = None
        retryable_status_codes = set(
            kwargs.pop("retryable_status_codes", self.RETRYABLE_STATUS_CODES)
        )
        attempt_callback: Callable[[dict[str, Any]], None] | None = kwargs.pop(
            "attempt_callback", None
        )
        timeout = kwargs.pop("timeout", self.timeout)
        verify = kwargs.pop("verify", self._discovery.verify_tls)

        for attempt in range(1, max_attempts + 1):
            try:
                response = self._session().request(
                    method,
                    url,
                    timeout=timeout,
                    verify=verify,
                    **kwargs,
                )
                retryable = response.status_code in retryable_status_codes
                is_final = not retryable or attempt == max_attempts
                delay = None if is_final else self._retry_delay(response, backoff)
                if attempt_callback is not None:
                    attempt_callback(
                        {
                            "attempt_number": attempt,
                            "max_attempts": max_attempts,
                            "status_code": response.status_code,
                            "outcome": "SUCCESS" if not retryable else ("EXHAUSTED" if is_final else "RETRY"),
                            "retry_delay_seconds": delay,
                            "error_type": None,
                            "error_message": None,
                        }
                    )
                if is_final:
                    return response
                LOGGER.warning(
                    "HTTP %s para %s; reintento %s/%s en %.1fs",
                    response.status_code,
                    url,
                    attempt,
                    max_retries,
                    delay,
                )
                response.close()
                time.sleep(float(delay))
                backoff = min(backoff * 2, self._download.max_backoff_seconds)
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = exc
                is_final = attempt == max_attempts
                delay = None if is_final else min(backoff, self._download.max_backoff_seconds)
                if attempt_callback is not None:
                    attempt_callback(
                        {
                            "attempt_number": attempt,
                            "max_attempts": max_attempts,
                            "status_code": None,
                            "outcome": "EXHAUSTED" if is_final else "RETRY",
                            "retry_delay_seconds": delay,
                            "error_type": type(exc).__name__,
                            "error_message": str(exc)[:2000],
                        }
                    )
                if is_final:
                    raise
                LOGGER.warning(
                    "Error temporal para %s; reintento %s/%s en %.1fs: %s",
                    url,
                    attempt,
                    max_retries,
                    delay,
                    exc,
                )
                time.sleep(float(delay))
                backoff = min(backoff * 2, self._download.max_backoff_seconds)

        if last_error is not None:
            raise last_error
        raise RuntimeError("El cliente HTTP agotó los reintentos sin respuesta")

    @staticmethod
    def _retry_delay(response: requests.Response, fallback: float) -> float:
        retry_after = response.headers.get("Retry-After")
        if not retry_after:
            return fallback
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            try:
                parsed = email.utils.parsedate_to_datetime(retry_after)
                now = datetime.now(timezone.utc)
                return max(0.0, (parsed - now).total_seconds())
            except (TypeError, ValueError, OverflowError):
                return fallback

    def get_text(self, url: str) -> str:
        response = self.request("GET", url)
        try:
            response.raise_for_status()
            return response.text
        finally:
            response.close()

    def close(self) -> None:
        with self._sessions_lock:
            sessions = list(self._sessions)
            self._sessions.clear()
        for session in sessions:
            session.close()