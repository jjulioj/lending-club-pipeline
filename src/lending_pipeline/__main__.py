"""CLI do pipeline: `python -m lending_pipeline <comando>`.

Fluxo típico:
  profile          -> relatório do arquivo bruto (docs/profiling_report.md)
  simulate         -> gera as cargas em data/landing a partir do arquivo bruto
  run              -> processa as cargas pendentes (bronze -> silver -> gold)
  run --batch X --force -> reprocessa uma carga específica
  report           -> indicadores e qualidade em docs/kpi_report.md
  demo-reprocess   -> evidência de idempotência em docs/evidence_reprocessamento.md
  status           -> situação de cada carga na tabela de controle
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import load_config
from .logging_utils import setup_logging

log = logging.getLogger("lending_pipeline")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lending_pipeline", description="Pipeline da carteira de empréstimos")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("profile", help="Gera o relatório de profiling do arquivo bruto")
    p.add_argument("--raw-file", type=Path, help="Sobrescreve o caminho do arquivo bruto")
    p.add_argument("--output", type=Path, help="Arquivo markdown de saída")

    p = sub.add_parser("simulate", help="Gera as cargas simuladas em landing a partir do arquivo bruto")
    p.add_argument("--raw-file", type=Path, help="Sobrescreve o caminho do arquivo bruto")

    p = sub.add_parser("run", help="Processa as cargas pendentes e reconstrói a gold")
    p.add_argument("--batch", action="append", dest="batches", help="Processa só esta carga (pode repetir)")
    p.add_argument("--force", action="store_true", help="Reprocessa mesmo se já processada com o mesmo conteúdo")
    p.add_argument("--no-gold", action="store_true", help="Não reconstrói a gold ao final")

    sub.add_parser("gold", help="Reconstrói só a camada gold a partir da silver")
    sub.add_parser("report", help="Gera docs/kpi_report.md a partir da gold")
    sub.add_parser("status", help="Mostra a situação de cada carga")

    p = sub.add_parser("demo-reprocess", help="Reexecuta uma carga já processada e registra a evidência")
    p.add_argument("--batch", required=True, help="Carga a reexecutar (ex.: batch_005_2011)")

    return parser


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    args = build_parser().parse_args(argv)
    config = load_config()

    from .spark import get_spark

    if args.command == "profile":
        from .profiling import run_profile

        spark = get_spark(config, app_name="lending-profile", with_delta=False)
        run_profile(spark, config, raw_file=args.raw_file, output_file=args.output)
        return 0

    if args.command == "simulate":
        from .io import read_raw_csv, resolve_raw_file
        from .simulate import SimulationParams, plan_batches, summary_markdown, write_batches

        spark = get_spark(config, app_name="lending-simulate", with_delta=False)
        params = SimulationParams(
            update_fraction=config.sim_update_fraction,
            conflict_fraction=config.sim_conflict_fraction,
            duplicate_fraction=config.sim_duplicate_fraction,
            seed=config.sim_seed,
        )
        raw = read_raw_csv(spark, resolve_raw_file(args.raw_file or config.raw_file), config.csv_multiline)
        summary = write_batches(plan_batches(raw, params), config.landing_dir)
        _write(config.reports_dir / "simulation_summary.md", summary_markdown(summary, params))
        return 0

    spark = get_spark(config, app_name=f"lending-{args.command}")

    if args.command == "run":
        from .pipeline import run_pipeline

        run_summary = run_pipeline(
            spark, config, only_batches=args.batches, force=args.force, build_gold_after=not args.no_gold
        )
        return 1 if run_summary.failed else 0

    if args.command == "gold":
        from datetime import UTC, datetime

        from .gold import build_gold
        from .lake import tables

        t = tables(config)
        build_gold(spark, t.silver_loans, t.gold, datetime.now(UTC))
        return 0

    if args.command == "report":
        from .reporting import kpi_report

        _write(config.reports_dir / "kpi_report.md", kpi_report(spark, config))
        return 0

    if args.command == "status":
        from .reporting import status_table

        print(status_table(spark, config))
        return 0

    if args.command == "demo-reprocess":
        from .reporting import demo_reprocess

        _write(config.reports_dir / "evidence_reprocessamento.md", demo_reprocess(spark, config, args.batch))
        return 0

    log.error("Comando desconhecido: %s", args.command)
    return 1


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    log.info("Relatório salvo em %s", path)


if __name__ == "__main__":
    sys.exit(main())
