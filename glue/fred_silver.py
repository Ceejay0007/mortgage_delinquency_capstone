"""FRED Silver job. Turn raw FRED JSON (Bronze) into one clean table for the silver layer."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def read_bronze(spark, path): 
    """ Read every raw FRED JSON file under the given folder."""
    return spark.read.option("multiLine", "true").json(path)


def explode_observations(raw): 
    """ Unpack the observations list so each date becomes its own row."""
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
    """ Convert text columns to their real types and turn FRED's "." into a missing value."""
    return (
        obs.withColumn("observation_date", F.to_date("observation_date"))
        .withColumn(
            "value",
            F.when(F.col("value") == ".", None).otherwise(F.col("value").cast("double")),
        )
        .withColumn("is_missing", F.col("value").isNull())
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("fred_silver_local").getOrCreate()

    raw = read_bronze(spark, "data/bronze/fred/")

    expected = raw.agg(F.sum("count")).first()[0]

    obs = explode_observations(raw)

    clean = convert_types(obs)
    print("Expected rows:", expected)
    print("Actual rows:  ", clean.count())
    clean.printSchema()

    missing = clean.filter(F.col("is_missing"))
    print("Missing values:", missing.count())
    missing.select("series_id", "observation_date").show(5)

    spark.stop()