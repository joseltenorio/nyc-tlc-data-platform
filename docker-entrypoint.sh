#!/bin/sh
set -eu

if [ "${1:-}" = "pytest" ]; then
  exec "$@"
fi

exec python -m tlc_data_platform "$@"
