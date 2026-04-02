"""
Job Spark — Agrégations horaires des données de parkings Rennes.

Lit les snapshots bruts JSON depuis MinIO (bucket parkings-raw),
calcule des agrégations par parking et par heure, puis écrit
les résultats en Parquet dans MinIO (bucket parkings-processed).

Usage :
    spark-submit spark_jobs/transform_parking.py

    # Avec les JARs S3A nécessaires :
    spark-submit \
      --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
      spark_jobs/transform_parking.py
"""
import os
import logging
import sys
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("spark_parking")


def create_spark_session():
    """Crée et configure la SparkSession avec le connecteur S3A pour MinIO."""
    from pyspark.sql import SparkSession

    endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    # S3A attend une URL complète (sans le schéma si MINIO_SECURE=false)
    endpoint_url: str = f"http://{endpoint}" if not endpoint.startswith("http") else endpoint

    spark = (
        SparkSession.builder
        .appName("ParkingsRennesAggregations")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Désactive la vérification de version Hadoop pour les envs locaux
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession créée — Spark %s", spark.version)
    return spark


def build_source_paths(bucket_raw: str, hours_back: int = 24) -> list[str]:
    """Retourne les chemins S3A à lire pour les N dernières heures.

    Construit des préfixes par source (citedia / star) avec une wildcard
    pour que Spark lise l'ensemble des fichiers JSON disponibles.
    """
    paths = [
        f"s3a://{bucket_raw}/citedia/",
        f"s3a://{bucket_raw}/star/",
    ]
    logger.info("Sources : %s", paths)
    return paths


def run_aggregations(spark, bucket_raw: str, bucket_processed: str) -> None:
    """Charge les JSON bruts, applique les transformations, écrit en Parquet."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, IntegerType, DoubleType, TimestampType,
    )

    paths = build_source_paths(bucket_raw)

    # ── 1. Lecture des fichiers JSON bruts ────────────────────────────────────
    logger.info("Lecture des fichiers JSON depuis MinIO…")

    # Schéma commun aux deux sources après normalisation côté Python
    schema = StructType([
        StructField("parking_id",      StringType(),   True),
        StructField("name",            StringType(),   True),
        StructField("source",          StringType(),   True),
        StructField("type",            StringType(),   True),
        StructField("lat",             DoubleType(),   True),
        StructField("lon",             DoubleType(),   True),
        StructField("total_spaces",    IntegerType(),  True),
        StructField("free_spaces",     IntegerType(),  True),
        StructField("occupied_spaces", IntegerType(),  True),
        StructField("status",          StringType(),   True),
        StructField("is_open",         StringType(),   True),  # bool → string en JSON
        StructField("occupancy_rate",  DoubleType(),   True),
        StructField("snapshot_time",   StringType(),   True),
        StructField("temperature_c",   DoubleType(),   True),
    ])

    try:
        # Lecture multi-chemins (Spark gère les fichiers manquants)
        df_raw = (
            spark.read
            .option("multiline", "true")
            .option("mode", "DROPMALFORMED")
            .json(paths)
        )
    except Exception as exc:
        logger.error("Impossible de lire depuis MinIO : %s", exc)
        raise

    if df_raw.rdd.isEmpty():
        logger.warning("Aucune donnée disponible dans les buckets source — abandon.")
        return

    logger.info("Lignes brutes lues : %d", df_raw.count())

    # ── 2. Nettoyage et cast des types ────────────────────────────────────────
    df = (
        df_raw
        .filter(F.col("parking_id").isNotNull())
        .withColumn("snapshot_ts",
                    F.to_timestamp(F.col("snapshot_time")))
        .withColumn("hour_bucket",
                    F.date_trunc("hour", F.col("snapshot_ts")))
        .withColumn("free_spaces",
                    F.col("free_spaces").cast(IntegerType()))
        .withColumn("occupied_spaces",
                    F.col("occupied_spaces").cast(IntegerType()))
        .withColumn("total_spaces",
                    F.col("total_spaces").cast(IntegerType()))
        .withColumn("occupancy_rate",
                    F.col("occupancy_rate").cast(DoubleType()))
        .dropna(subset=["snapshot_ts", "parking_id"])
    )

    # ── 3. Agrégation horaire par parking ─────────────────────────────────────
    logger.info("Calcul des agrégations horaires…")
    df_hourly = (
        df.groupBy("hour_bucket", "parking_id", "name", "source", "type", "lat", "lon")
        .agg(
            F.round(F.avg("free_spaces"),     0).cast(IntegerType()).alias("avg_free_spaces"),
            F.round(F.avg("occupied_spaces"), 0).cast(IntegerType()).alias("avg_occupied_spaces"),
            F.first("total_spaces").alias("total_spaces"),
            F.round(F.avg("occupancy_rate"),  1).alias("avg_occupancy_rate"),
            F.min("free_spaces").alias("min_free_spaces"),
            F.max("free_spaces").alias("max_free_spaces"),
            F.round(F.avg("temperature_c"),   1).alias("avg_temperature_c"),
            F.count("*").alias("snapshot_count"),
        )
        .orderBy("hour_bucket", "parking_id")
    )

    # ── 4. Moyenne mobile sur 3h (fenêtre glissante) ──────────────────────────
    from pyspark.sql.window import Window

    window_3h = (
        Window
        .partitionBy("parking_id")
        .orderBy(F.col("hour_bucket").cast("long"))
        .rangeBetween(-3 * 3600, 0)  # 3 heures en secondes
    )

    df_final = df_hourly.withColumn(
        "rolling_avg_free_3h",
        F.round(F.avg("avg_free_spaces").over(window_3h), 0).cast(IntegerType()),
    )

    rows = df_final.count()
    logger.info("Agrégations calculées : %d lignes", rows)

    # ── 5. Écriture en Parquet partitionné par date ───────────────────────────
    output_path = f"s3a://{bucket_processed}/hourly_aggregations/"
    logger.info("Écriture Parquet → %s", output_path)

    (
        df_final
        .withColumn("date", F.to_date("hour_bucket"))
        .write
        .mode("overwrite")
        .partitionBy("date", "source")
        .parquet(output_path)
    )

    logger.info("Job terminé — %d agrégations écrites dans %s", rows, output_path)


def main() -> None:
    """Point d'entrée principal du job Spark."""
    bucket_raw: str       = os.getenv("MINIO_BUCKET_RAW",       "parkings-raw")
    bucket_processed: str = os.getenv("MINIO_BUCKET_PROCESSED", "parkings-processed")

    logger.info("=" * 60)
    logger.info("Job Spark — Agrégations Parkings Rennes")
    logger.info("Source      : s3a://%s/", bucket_raw)
    logger.info("Destination : s3a://%s/hourly_aggregations/", bucket_processed)
    logger.info("=" * 60)

    spark = create_spark_session()
    try:
        run_aggregations(spark, bucket_raw, bucket_processed)
    finally:
        spark.stop()
        logger.info("SparkSession arrêtée.")


if __name__ == "__main__":
    main()
