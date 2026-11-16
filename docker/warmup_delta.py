"""Executado no `docker build`: baixa os jars do Delta e faz uma escrita/leitura de teste."""

import os
import tempfile

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.master("local[1]")
    .appName("delta-warmup")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.ivy", os.environ.get("SPARK_JARS_IVY", "/opt/ivy"))
    .config("spark.ui.enabled", "false")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

with tempfile.TemporaryDirectory() as tmp:
    path = f"{tmp}/warmup"
    spark.range(3).write.format("delta").save(path)
    assert spark.read.format("delta").load(path).count() == 3

spark.stop()
print("Delta OK")
