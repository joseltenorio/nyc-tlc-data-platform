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


def _is_html_response(response: requests.Response) -> bool:
    content_type = (response.headers.get("Content-Type") or "").lower()
    return "text/html" in content_type


class RemoteProbe:
    def __init__(self, http: HttpClient) -> None:
        self._http = http

    def probe(self, url: str) -> RemoteMetadata:
        try:
            response = self._http.request("HEAD", url, allow_redirects=True)
            try:
                if response.status_code == 200 and not _is_html_response(response):
                    return _metadata(response, True)
                if response.status_code in {404, 410}:
                    return _metadata(response, False)
                if response.status_code not in {202, 403, 405} and not _is_html_response(
                    response
                ):
                    return _metadata(response, False)
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
                available = response.status_code in {200, 206} and not _is_html_response(
                    response
                )
                return _metadata(response, available)
            finally:
                response.close()
        except requests.RequestException as exc:
            return RemoteMetadata(
                available=False,
                probe_failed=True,
                error_message=str(exc),
            )