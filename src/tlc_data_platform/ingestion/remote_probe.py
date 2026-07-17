from __future__ import annotations

import requests

from tlc_data_platform.bronze.models import RemoteMetadata
from tlc_data_platform.ingestion.http_client import HttpClient


def _metadata(response: requests.Response, available: bool) -> RemoteMetadata:
    content_length = response.headers.get("Content-Length")
    return RemoteMetadata(
        available=available,
        status_code=response.status_code,
        content_length=int(content_length) if content_length and content_length.isdigit() else None,
        etag=response.headers.get("ETag"),
        last_modified=response.headers.get("Last-Modified"),
        content_type=response.headers.get("Content-Type"),
    )


class RemoteProbe:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def probe(self, url: str) -> RemoteMetadata:
        try:
            response = self._http.request("HEAD", url, allow_redirects=True)
            try:
                if response.status_code == 200:
                    return _metadata(response, True)
                if response.status_code in {404, 410}:
                    return _metadata(response, False)
                if response.status_code not in {403, 405}:
                    response.raise_for_status()
            finally:
                response.close()

            response = self._http.request(
                "GET",
                url,
                headers={"Range": "bytes=0-0"},
                stream=True,
                allow_redirects=True,
            )
            try:
                available = response.status_code in {200, 206}
                if not available and response.status_code not in {404, 410}:
                    response.raise_for_status()
                return _metadata(response, available)
            finally:
                response.close()
        except requests.RequestException as exc:
            return RemoteMetadata(
                available=False,
                probe_failed=True,
                error_message=str(exc),
            )
