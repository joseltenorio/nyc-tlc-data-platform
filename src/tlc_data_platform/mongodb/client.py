from __future__ import annotations

import os
from typing import Any

from tlc_data_platform.core.settings import MongoConfig


class MongoClientProvider:
    def __init__(self, config: MongoConfig) -> None:
        self._config = config
        self._client: Any | None = None
        self._database: Any | None = None

    def database(self) -> Any:
        if self._database is None:
            from pymongo import MongoClient

            uri = os.getenv(
                self._config.uri_environment_variable,
                self._config.default_uri,
            )
            self._client = MongoClient(
                uri,
                serverSelectionTimeoutMS=self._config.connect_timeout_ms,
            )
            self._client.admin.command("ping")
            self._database = self._client[self._config.database]
        return self._database

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            self._database = None
