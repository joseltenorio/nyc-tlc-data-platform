#!/bin/sh
set -eu

case "${1:-}" in
  pytest|streamlit|python|sh|bash)
    exec "$@"
    ;;
  *)
    exec python -m tlc_data_platform "$@"
    ;;
esac
