from __future__ import annotations

import logging


def configure_logging(level: str, log_format: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper()), format=log_format)