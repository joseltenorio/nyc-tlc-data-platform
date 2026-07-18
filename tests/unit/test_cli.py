from tlc_data_platform.cli.main import build_parser


def test_cli_supports_required_commands():
    parser = build_parser()
    assert parser.parse_args(["historical"]).command == "historical"
    assert parser.parse_args(["incremental"]).command == "incremental"
    assert parser.parse_args(["plan"]).command == "plan"
    args = parser.parse_args(["run", "--dry-run", "--workers", "4"])
    assert args.dry_run is True and args.workers == 4
    assert parser.parse_args(["silver-plan"]).command == "silver-plan"
    assert parser.parse_args(["silver-historical"]).command == "silver-historical"
    assert parser.parse_args(["silver-references"]).command == "silver-references"
    assert parser.parse_args(["medallion-incremental"]).command == "medallion-incremental"
    assert parser.parse_args(["gold-plan"]).command == "gold-plan"
    assert parser.parse_args(["gold-historical"]).command == "gold-historical"
    assert parser.parse_args(["ml-plan", "--models", "forecast"]).models == ["forecast"]
    assert parser.parse_args(["ml-train", "--models", "wait-risk"]).models == ["wait-risk"]
    platform = parser.parse_args(["platform-incremental", "--train-ml"])
    assert platform.train_ml is True
