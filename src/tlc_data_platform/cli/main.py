from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from tlc_data_platform.bronze.manifest import JsonEncoder
from tlc_data_platform.core.logging import configure_logging
from tlc_data_platform.core.settings import (
    load_config,
    resolve_selection,
    resolve_silver_selection,
)
from tlc_data_platform.orchestration.bronze_pipeline import (
    plan_bronze_pipeline,
    run_bronze_pipeline,
)
from tlc_data_platform.orchestration.medallion_pipeline import run_medallion_to_silver
from tlc_data_platform.orchestration.silver_pipeline import (
    plan_silver_pipeline,
    refresh_silver_references,
    run_silver_pipeline,
)

SERVICES = ("yellow", "green", "fhv", "fhvhv")


def _add_selection_arguments(parser: argparse.ArgumentParser, *, include_workers: bool) -> None:
    parser.add_argument(
        "--config-dir",
        default="config",
        help="Directorio con los YAML del proyecto.",
    )
    parser.add_argument("--services", nargs="+", choices=SERVICES)
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--months", nargs="+", type=int)
    if include_workers:
        parser.add_argument("--workers", type=int)
        parser.add_argument("--max-hvfhv-workers", type=int)
    parser.add_argument(
        "--continue-on-error",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"))


def _add_bronze_run_arguments(parser: argparse.ArgumentParser) -> None:
    _add_selection_arguments(parser, include_workers=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")


def _add_silver_run_arguments(parser: argparse.ArgumentParser) -> None:
    _add_selection_arguments(parser, include_workers=False)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--refresh-references",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Fuerza o desactiva la actualización de taxi_zones/base_lookup.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tlc-platform",
        description="NYC TLC Data Platform: ingesta Bronze y curación Silver con PySpark y MongoDB.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("plan", help="Plan de disponibilidad Bronze.")
    _add_selection_arguments(plan, include_workers=True)

    for name, help_text in (
        ("historical", "Carga Bronze histórica 2019-2025."),
        ("incremental", "Carga Bronze incremental 2026."),
        ("run", "Carga Bronze para un rango explícito."),
    ):
        command = subparsers.add_parser(name, help=help_text)
        _add_bronze_run_arguments(command)

    silver_plan = subparsers.add_parser("silver-plan", help="Plan Bronze READY -> Silver.")
    _add_selection_arguments(silver_plan, include_workers=False)

    for name, help_text in (
        ("silver-historical", "Transforma a Silver el histórico 2019-2025."),
        ("silver-incremental", "Transforma a Silver los meses disponibles de 2026."),
        ("silver-run", "Transforma a Silver un rango explícito."),
    ):
        command = subparsers.add_parser(name, help=help_text)
        _add_silver_run_arguments(command)

    references = subparsers.add_parser(
        "silver-references",
        help="Actualiza Taxi Zone Lookup y Current Bases en Silver.",
    )
    references.add_argument("--config-dir", default="config")
    references.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"))

    for name, help_text in (
        ("medallion-historical", "Ejecuta Bronze y luego Silver para 2019-2025."),
        ("medallion-incremental", "Ejecuta Bronze y luego Silver para 2026."),
        ("medallion-run", "Ejecuta Bronze y Silver para un rango explícito."),
    ):
        command = subparsers.add_parser(name, help=help_text)
        _add_selection_arguments(command, include_workers=True)
        command.add_argument("--force-bronze", action="store_true")
        command.add_argument("--force-silver", action="store_true")
        command.add_argument(
            "--refresh-references",
            action=argparse.BooleanOptionalAction,
            default=None,
        )

    return parser


def _print_result(result: object) -> None:
    payload = result.to_dict()  # type: ignore[attr-defined]
    print(json.dumps(payload, cls=JsonEncoder, ensure_ascii=False, indent=2))


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_config(Path(args.config_dir))
    configure_logging(args.log_level or config.logging.level, config.logging.format)

    try:
        if args.command == "silver-references":
            result = refresh_silver_references(config)
            _print_result(result)
            return 0 if result.status == "SUCCESS" else 2

        if args.command.startswith("silver-"):
            selection = resolve_silver_selection(
                config,
                mode=args.command,
                services=args.services,
                start_year=args.start_year,
                end_year=args.end_year,
                months=args.months,
                continue_on_error=args.continue_on_error,
            )
            if args.command == "silver-plan":
                result = plan_silver_pipeline(config, selection)
                blocking = any(
                    warning.startswith("MongoDB no estuvo disponible")
                    or warning.startswith("Faltan taxi_zones")
                    for warning in result.warnings
                )
                exit_code = 2 if blocking else 0
            else:
                result = run_silver_pipeline(
                    config,
                    selection,
                    execution_type=args.command.removeprefix("silver-"),
                    force=args.force,
                    refresh_references=args.refresh_references,
                )
                exit_code = 0 if result.status in {"SUCCESS", "NO_INPUT"} else 2
            _print_result(result)
            return exit_code

        if args.command.startswith("medallion-"):
            base_mode = args.command.removeprefix("medallion-")
            selection = resolve_selection(
                config,
                mode=base_mode,
                services=args.services,
                start_year=args.start_year,
                end_year=args.end_year,
                months=args.months,
                workers=args.workers,
                max_hvfhv_workers=args.max_hvfhv_workers,
                continue_on_error=args.continue_on_error,
            )
            result = run_medallion_to_silver(
                config,
                selection,
                execution_type=base_mode,
                force_bronze=args.force_bronze,
                force_silver=args.force_silver,
                refresh_references=args.refresh_references,
            )
            _print_result(result)
            return 0 if result.status == "SUCCESS" else 2

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
        _print_result(result)
        return exit_code
    except Exception as exc:
        import logging

        logging.getLogger(__name__).exception("La ejecución falló: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
