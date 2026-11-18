"""FRED Silver job: turn raw FRED JSON (Bronze) into one clean table for the Silver layer.

Run locally (reads data/bronze, writes data/silver):
    python glue/fred_silver.py

Run in AWS Glue (reads and writes S3):
    Job parameter --BUCKET <bucket-name>
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# National series: series_id -> (measure, frequency)
NATIONAL_SERIES = {
    "UNRATE": ("unemployment_rate", "monthly"),
    "MORTGAGE30US": ("mortgage_rate_30y", "weekly"),
    "USSTHPI": ("house_price_index", "quarterly"),
    "FEDFUNDS": ("fed_funds_rate", "monthly"),
    "CPIAUCSL": ("cpi", "monthly"),
    "GDPC1": ("real_gdp", "quarterly"),
    "USREC": ("recession_indicator", "monthly"),
}

# Final column order for the Silver table
SILVER_COLUMNS = [
    "series_id", "measure", "geo_level", "state_code", "frequency",
    "observation_date", "value", "is_missing", "ingest_date",
]


def get_paths():
    """Use S3 when --BUCKET is given (Glue); otherwise use local folders (laptop)."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--BUCKET")
    args, _ = parser.parse_known_args()
    base = f"s3://{args.BUCKET}" if args.BUCKET else "data"
    return f"{base}/bronze/fred/", f"{base}/silver/fred_observations/"


def read_bronze(spark, path):
    """Read every raw FRED JSON file under the given folder."""
    return spark.read.option("multiLine", "true").json(path)


def keep_latest(raw):
    """Keep only the newest download (ingest_date) of each series."""
    latest = raw.groupBy("series_id").agg(F.max("ingest_date").alias("ingest_date"))
    return raw.join(latest, on=["series_id", "ingest_date"], how="inner")


def explode_observations(raw):
    """Unpack the observations list so each date becomes its own row."""
    return raw.select(
        "series_id",
        "ingest_date",
        F.explode("observations").alias("obs"),
    ).select(
        "series_id",
        "ingest_date",
        F.col("obs.date").alias("observation_date"),
        F.col("obs.value").alias("value"),
    )


def convert_types(obs):
    """Convert text columns to their real types and turn FRED's "." into a missing value."""
    return (
        obs.withColumn("observation_date", F.to_date("observation_date"))
        .withColumn(
            "value",
            F.when(F.col("value") == ".", None).otherwise(F.col("value").cast("double")),
        )
        .withColumn("is_missing", F.col("value").isNull())
    )


def add_labels(df):
    """Label each series with what it measures, how often, and where."""
    sid = F.col("series_id")
    national = sid.isin(list(NATIONAL_SERIES))

    # State series follow a naming pattern: TXUR = Texas unemployment, TXSTHPI = Texas house prices
    measure = F.when(sid.rlike("UR$"), "unemployment_rate").when(sid.rlike("STHPI$"), "house_price_index")
    frequency = F.when(sid.rlike("UR$"), "monthly").when(sid.rlike("STHPI$"), "quarterly")

    # National series override the pattern (USSTHPI ends in STHPI but is national, not a state)
    for code, (m, f) in NATIONAL_SERIES.items():
        measure = F.when(sid == code, m).otherwise(measure)
        frequency = F.when(sid == code, f).otherwise(frequency)

    return (
        df.withColumn("measure", measure)
        .withColumn("frequency", frequency)
        .withColumn("geo_level", F.when(national, "national").when(measure.isNotNull(), "state").otherwise("unknown"))
        .withColumn("state_code", F.when(~national & measure.isNotNull(), F.substring("series_id", 1, 2)))
    )


def quality_checks(df):
    """Stop the job if the Silver table breaks any rule."""
    problems = []

    if df.count() == 0:
        problems.append("table is empty")

    dupes = df.groupBy("series_id", "observation_date").count().filter("count > 1").count()
    if dupes:
        problems.append(f"{dupes} duplicate series/date pairs")

    unknown = df.filter(F.col("geo_level") == "unknown").select("series_id").distinct().count()
    if unknown:
        problems.append(f"{unknown} unrecognized series")

    bad_dates = df.filter(F.col("observation_date").isNull()).count()
    if bad_dates:
        problems.append(f"{bad_dates} rows with unreadable dates")

    if problems:
        raise ValueError("Quality checks failed: " + "; ".join(problems))
    print("Quality checks passed")


def write_silver(df, path):
    """Write the clean table as Parquet, replacing the previous version."""
    (
        df.withColumn("processed_at", F.current_timestamp())
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(path)
    )


if __name__ == "__main__":
    bronze_path, silver_path = get_paths()
    spark = SparkSession.builder.appName("fred_silver").getOrCreate()

    raw = keep_latest(read_bronze(spark, bronze_path))
    expected = raw.agg(F.sum("count")).first()[0]

    silver = add_labels(convert_types(explode_observations(raw))).select(*SILVER_COLUMNS)
    actual = silver.count()
    print(f"Expected rows: {expected}  Actual rows: {actual}")
    if actual != expected:
        raise ValueError(f"Row count mismatch: expected {expected}, got {actual}")

    quality_checks(silver)
    write_silver(silver, silver_path)
    print(f"Wrote {actual} rows to {silver_path}")

    spark.stop()