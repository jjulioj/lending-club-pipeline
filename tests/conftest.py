"""Fixtures compartilhadas dos testes.

Uma SparkSession por execução do pytest (criar sessão é caro). Testes que
precisam de Delta usam o marcador `delta`; em ambientes sem os jars do Delta
dá para pulá-los com LENDING_TEST_DELTA=0 (no Docker e no CI eles rodam).
"""

from __future__ import annotations

import os

import pytest

from lending_pipeline.config import load_config

DELTA_ENABLED = os.environ.get("LENDING_TEST_DELTA", "1") != "0"


def pytest_collection_modifyitems(config, items):
    if DELTA_ENABLED:
        return
    skip = pytest.mark.skip(reason="LENDING_TEST_DELTA=0: Delta indisponível neste ambiente")
    for item in items:
        if "delta" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def config(tmp_path_factory):
    """Config apontando para diretórios temporários: testes nunca tocam data/."""
    base = tmp_path_factory.mktemp("lake")
    os.environ["LENDING_PATHS_LANDING_DIR"] = str(base / "landing")
    os.environ["LENDING_PATHS_LAKE_DIR"] = str(base / "lake")
    os.environ["LENDING_PATHS_REPORTS_DIR"] = str(base / "reports")
    os.environ.setdefault("LENDING_SPARK_MASTER", "local[2]")
    os.environ.setdefault("LENDING_SPARK_SHUFFLE_PARTITIONS", "2")
    os.environ.setdefault("LENDING_SPARK_DRIVER_MEMORY", "2g")
    return load_config()


@pytest.fixture(scope="session")
def spark(config):
    from lending_pipeline.spark import get_spark

    session = get_spark(config, app_name="lending-tests", with_delta=DELTA_ENABLED)
    yield session
    session.stop()
