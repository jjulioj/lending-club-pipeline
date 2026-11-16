"""Carregamento de configuração.

Ordem de precedência: variável de ambiente > config/pipeline.toml > default.
Variáveis de ambiente seguem o padrão LENDING_<SEÇÃO>_<CHAVE>, por exemplo
LENDING_PATHS_RAW_FILE ou LENDING_SPARK_DRIVER_MEMORY.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_FILE = PROJECT_ROOT / "config" / "pipeline.toml"


@dataclass(frozen=True)
class Config:
    raw_file: Path
    landing_dir: Path
    lake_dir: Path
    reports_dir: Path
    spark_master: str
    spark_driver_memory: str
    spark_shuffle_partitions: int
    csv_multiline: bool
    max_quarantine_rate: float
    sim_update_fraction: float
    sim_conflict_fraction: float
    sim_duplicate_fraction: float
    sim_seed: int


def _get(raw: dict, section: str, key: str, default):
    env_name = f"LENDING_{section}_{key}".upper()
    if env_name in os.environ:
        return os.environ[env_name]
    return raw.get(section, {}).get(key, default)


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _resolve(path_value: str) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def load_config(config_file: Path | None = None) -> Config:
    config_file = config_file or Path(os.environ.get("LENDING_CONFIG_FILE", DEFAULT_CONFIG_FILE))
    raw: dict = {}
    if config_file.exists():
        with config_file.open("rb") as fh:
            raw = tomllib.load(fh)

    return Config(
        raw_file=_resolve(_get(raw, "paths", "raw_file", "data/raw/accepted_2007_to_2018Q4.csv.gz")),
        landing_dir=_resolve(_get(raw, "paths", "landing_dir", "data/landing")),
        lake_dir=_resolve(_get(raw, "paths", "lake_dir", "data/lake")),
        reports_dir=_resolve(_get(raw, "paths", "reports_dir", "docs")),
        spark_master=str(_get(raw, "spark", "master", "local[*]")),
        spark_driver_memory=str(_get(raw, "spark", "driver_memory", "4g")),
        spark_shuffle_partitions=int(_get(raw, "spark", "shuffle_partitions", 16)),
        csv_multiline=_as_bool(_get(raw, "csv", "multiline", True)),
        max_quarantine_rate=float(_get(raw, "quality", "max_quarantine_rate", 0.05)),
        sim_update_fraction=float(_get(raw, "simulation", "update_fraction", 0.30)),
        sim_conflict_fraction=float(_get(raw, "simulation", "conflict_fraction", 0.002)),
        sim_duplicate_fraction=float(_get(raw, "simulation", "duplicate_fraction", 0.001)),
        sim_seed=int(_get(raw, "simulation", "seed", 42)),
    )
