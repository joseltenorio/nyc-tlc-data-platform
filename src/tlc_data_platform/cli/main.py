from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from tlc_data_platform.bronze.manifest import JsonEncoder
from tlc_data_platform.core.logging import configure_logging
from tlc_data_platform.core.settings import load_config, resolve_selection
from tlc_data_platform.orchestration.bronze_pipeline import (
    plan_bronze_pipeline,
    run_bronze_pipeline,
)

SERVICES = ("yellow", "green", "fhv", "fhvhv")


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config-dir",
        default="config",
        help="Directorio con app.yml, tlc_sources.yml, bronze.yml y schema_contracts.yml.",
    )
    parser.add_argument("--services", nargs="+", choices=SERVICES)
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--months", nargs="+", type=int)
    parser.add_argument("--workers", type=int)
    parser.add_argument("--max-hvfhv-workers", type=int)
    parser.add_argument(
        "--continue-on-error",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Continúa o detiene la publicación después de un archivo fallido.",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )


def _add_run_arguments(parser: argparse.ArgumentParser) -> None:
    _add_common_arguments(parser)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Descubre, prueba disponibilidad y audita sin descargar.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Vuelve a descargar aunque el periodo figure como READY.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tlc-platform",
        description="Pipeline Bronze de NYC TLC con PySpark, MongoDB y descarga concurrente.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("plan", help="Calcula cobertura, pendientes, volumen y espacio.")
    _add_common_arguments(plan)

    historical = subparsers.add_parser(
        "historical", help="Ejecuta la carga histórica configurada (2019-2025)."
    )
    _add_run_arguments(historical)

    incremental = subparsers.add_parser(
        "incremental", help="Ejecuta la carga incremental configurada (2026)."
    )
    _add_run_arguments(incremental)

    run = subparsers.add_parser(
        "run", help="Ejecuta un rango explícito o el rango completo configurado."
    )
    _add_run_arguments(run)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_config(Path(args.config_dir))
    configure_logging(args.log_level or config.logging.level, config.logging.format)

    selection = resolve_selection(
        config,
        mode=args.command,
        services=args.services,
        start_year=args.start_year,
        end_year=args.end_year,
        months=args.months,
        workers=args.workers,
        max_hvfhv_workers=args.max_hvfhv_workers,
        continue_on_error=args.continue_on_error,
    )

    try:
        if args.command == "plan":
            result = plan_bronze_pipeline(config, selection)
            exit_code = 0 if result.failed_probes == 0 else 2
        else:
            result = run_bronze_pipeline(
                config,
                selection,
                execution_type=args.command,
                dry_run=args.dry_run,
                force=args.force,
            )
            exit_code = 0 if result.status in {"SUCCESS", "DRY_RUN"} else 2
        print(
            json.dumps(
                result.to_dict(),
                cls=JsonEncoder,
                ensure_ascii=False,
                indent=2,
            )
        )
        return exit_code
    except Exception as exc:
        import logging

        logging.getLogger(__name__).exception("La ejecución Bronze falló: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
