"""
Freddie Mac Silver job
Turns the raw Freddie Mac text files (Bronze) into clean Parquet tables (Silver).
This job has four steps:
  1) READ   the pipe delimited files and name the columns
  2) CLEAN  replace "not available" codes, convert types, add helper columns
  3) CHECK  stop the job if anything looks wrong
  4) WRITE  save one Parquet folder per vintage, then confirm the row counts

Run on my laptop as  python glue/freddie_mac_silver.py   (reads ./data)
Run in AWS Glue as   Glue passes --BUCKET, so the same code reads and writes S3
"""
import argparse
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# SETTINGS

# Each column's "not available" code. These values become null as freddie mac usere guide says.
ORIG_NOT_AVAILABLE = {
    "credit_score": "9999", "vantage_score": "9999",
    "orig_dti": "999", "orig_ltv": "999", "orig_cltv": "999", "mi_pct": "999",
    "num_units": "99", "num_borrowers": "99", "property_type": "99",
    "first_time_homebuyer_flag": "9", "occupancy_status": "9",
    "channel": "9", "loan_purpose": "9",
    "property_valuation_method": "7",
}
PERF_NOT_AVAILABLE = {
    "estimated_ltv": "999",
    "mi_cancellation_flag": "7",
    "delinquency_status": "XX",
}

# The type each column becomes. Columns not listed stay as text (IDs, codes, names).
ORIG_TYPES = {
    "credit_score": "int", "vantage_score": "int", "mi_pct": "int", "num_units": "int",
    "orig_cltv": "int", "orig_dti": "int", "orig_ltv": "int",
    "orig_loan_term": "int", "num_borrowers": "int",
    "orig_upb": "double", "orig_interest_rate": "double",
    "first_payment_date": "date", "maturity_date": "date",
}

PERF_MONEY_COLUMNS = [
    "current_upb", "current_interest_rate", "non_interest_bearing_upb", "interest_bearing_upb",
    "mi_recoveries", "net_sales_proceeds", "non_mi_recoveries", "total_expenses", "legal_costs",
    "maintenance_costs", "taxes_insurance", "misc_expenses", "actual_loss", "cumulative_mod_costs",
    "current_period_mod_costs", "zero_balance_removal_upb", "delinquent_accrued_interest",
    "bankruptcy_cramdown_costs",
]
PERF_TYPES = {
    "loan_age": "int", "remaining_months": "int", "estimated_ltv": "int",
    "period": "date", "zero_balance_date": "date",
    "defect_settlement_date": "date", "last_paid_installment_date": "date",
}
PERF_TYPES.update({column: "double" for column in PERF_MONEY_COLUMNS})


# Step 1) READ

def get_paths():
    """Read from ./data on my laptop, or from S3 when Glue passes --BUCKET."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--BUCKET", default=None)
    parser.add_argument("--RELEASE", default="2026Q2")
    args, _ = parser.parse_known_args()  # ignore the extra arguments Glue adds

    base = f"s3://{args.BUCKET}" if args.BUCKET else "data"
    bronze = f"{base}/bronze/freddie_mac/release={args.RELEASE}"
    silver = f"{base}/silver/freddie_mac"
    return bronze, silver


def load_column_names():
    """Column names come from freddie_mac_columns.yaml (next to the script in Glue or in config/ locally)."""
    for path in [Path("freddie_mac_columns.yaml"), Path("config/freddie_mac_columns.yaml")]:
        if path.exists():
            layout = yaml.safe_load(path.read_text())
            orig_columns = [col["name"] for col in layout["origination"]]
            perf_columns = [col["name"] for col in layout["performance"]]
            return orig_columns, perf_columns
    raise FileNotFoundError("freddie_mac_columns.yaml not found")


def read_bronze(spark, folder, column_names):
    """Read pipe delimited text with no header, then give each column its name from the freddie_mac_columns.yaml file."""
    df = spark.read.csv(folder, sep="|", header=False)

    # Spark adds a "vintage" column from the vintage=YYYY folder names, so expect one extra
    if len(df.columns) != len(column_names) + 1:
        raise ValueError(f"{folder}: expected {len(column_names) + 1} columns, found {len(df.columns)}")

    renamed = [F.col(f"_c{i}").alias(name) for i, name in enumerate(column_names)]
    return df.select(*renamed, "vintage")


# Step 2) CLEAN


def convert(column, to_type):
    """How one text column is converted. Dates arrive as YYYYMM, so add '01' for the first of the month."""
    if to_type == "date":
        return F.to_date(F.concat(F.col(column), F.lit("01")), "yyyyMMdd")
    return F.col(column).cast(to_type)


def set_codes_to_null(df, codes):
    """Turn Freddie Mac's "not available" codes into real nulls.
    Freddie Mac fills missing values with placeholder numbers like 999 or 9999
    instead of leaving them blank. If those stayed in, a missing DTI would look
    like a DTI of 999% and distort averages and models. Only the exact code is
    replaced, so a real value such as a DTI of 9 is kept.
    """
    for column, code in codes.items():
        df = df.withColumn(column, F.when(F.col(column) == code, None).otherwise(F.col(column)))
    return df


def count_unconvertible(df, types):
    """Spark quietly turns text it cannot convert into null.  the script count those values so the job can stop."""
    counts = [
        F.sum(F.when(F.col(c).isNotNull() & convert(c, t).isNull(), 1).otherwise(0)).alias(c)
        for c, t in types.items()
    ]
    row = df.agg(*counts).first().asDict()
    return {column: n for column, n in row.items() if n > 0}


def convert_types(df, types):
    for column, to_type in types.items():
        df = df.withColumn(column, convert(column, to_type))
    return df


def clean_origination(raw):
    df = set_codes_to_null(raw, ORIG_NOT_AVAILABLE)
    bad_values = count_unconvertible(df, ORIG_TYPES)
    df = convert_types(df, ORIG_TYPES)

    # Before 2018Q2, num_borrowers = 2 meant "two or more", so cap every year at 2.
    # This runs after 99 became null, so a missing value is never turned into 2.
    df = df.withColumn("num_borrowers",
                       F.when(F.col("num_borrowers") > 2, 2).otherwise(F.col("num_borrowers")))

    df = df.withColumn("processed_at", F.current_timestamp())
    return df, bad_values


def clean_performance(raw):
    df = set_codes_to_null(raw, PERF_NOT_AVAILABLE)
    bad_values = count_unconvertible(df, PERF_TYPES)
    df = convert_types(df, PERF_TYPES)

    status = F.col("delinquency_status")
    df = (
        df
        # "03" becomes 3. RA is not a number, so it becomes null here...
        .withColumn("delinquency_months", F.when(status.rlike("^[0-9]+$"), status.cast("int")))
        # ...and is kept as its own true/false column, so the foreclosure signal is not lost
        .withColumn("is_reo", F.when(status == "RA", True).otherwise(False))
        # Keep Freddie Mac's flag codes, and add simple true/false versions
        .withColumn("is_modified",
                    F.when(F.col("modification_flag").isin("Y", "P"), True).otherwise(False))
        .withColumn("has_payment_deferral",
                    F.when(F.col("payment_deferral_flag").isin("C", "P"), True).otherwise(False))
        .withColumn("processed_at", F.current_timestamp())
    )
    return df, bad_values

# Step 3) CHECK


def rows_per_vintage(df):
    """Row counts per vintage, e.g. {2008: 50000, 2019: 50000}."""
    return {row["vintage"]: row["count"] for row in df.groupBy("vintage").count().collect()}


def check_origination(orig):
    """One row per loan, and every loan has an ID."""
    stats = orig.agg(
        F.count("*").alias("rows"),
        F.count("loan_id").alias("filled_ids"),
        F.countDistinct("loan_id").alias("loans"),
    ).first()

    if stats["rows"] == 0:
        return ["origination table is empty"]
    if not (stats["rows"] == stats["filled_ids"] == stats["loans"]):
        return [f"origination should have one row per loan: {stats['rows']} rows, "
                f"{stats['loans']} distinct loans, {stats['rows'] - stats['filled_ids']} blank IDs"]
    return []


def check_performance(perf):
    """One row per loan per month, and no status codes we do not recognise."""
    unknown_status = (F.col("delinquency_status").isNotNull()
                      & F.col("delinquency_months").isNull()
                      & ~F.col("is_reo"))
    stats = perf.agg(
        F.count("*").alias("rows"),
        F.countDistinct("loan_id", "period").alias("loan_months"),
        F.sum(F.when(unknown_status, 1).otherwise(0)).alias("unknown_status"),
    ).first()

    if stats["rows"] == 0:
        return ["performance table is empty"]
    problems = []
    if stats["rows"] != stats["loan_months"]:
        problems.append(f"{stats['rows'] - stats['loan_months']} duplicate loan-month rows")
    if stats["unknown_status"]:
        problems.append(f"{stats['unknown_status']} rows with an unknown delinquency status")
    return problems


def check_links(orig, perf):
    """Every loan in the performance file must exist in the origination file."""
    missing = (perf.select("loan_id").distinct()
               .join(orig.select("loan_id"), on="loan_id", how="left_anti")
               .count())
    return [f"{missing} performance loans have no origination record"] if missing else []


def run_checks(orig, perf, orig_bad_values, perf_bad_values):
    """Collect every problem first, then stop the job once with the full list."""
    problems = []
    if orig_bad_values:
        problems.append(f"origination values that could not be converted: {orig_bad_values}")
    if perf_bad_values:
        problems.append(f"performance values that could not be converted: {perf_bad_values}")
    problems += check_origination(orig)
    problems += check_performance(perf)
    problems += check_links(orig, perf)

    if problems:
        raise ValueError("Quality checks failed:\n- " + "\n- ".join(problems))


# Step 4) WRITE TO SILVER


def write_silver(df, path):
    """One Parquet folder per vintage. Rerunning a vintage replaces only that vintage."""
    df.repartition("vintage").write.mode("overwrite").partitionBy("vintage").parquet(path)


def confirm_written(spark, path, expected_counts):
    """Read back what was written and make sure no rows were lost."""
    written = spark.read.parquet(path).filter(F.col("vintage").isin(list(expected_counts)))
    found_counts = rows_per_vintage(written)
    if found_counts != expected_counts:
        raise ValueError(f"{path}: expected {expected_counts}, found {found_counts}")


# Run the four steps

def main():
    spark = SparkSession.builder.appName("freddie_mac_silver").getOrCreate()
    # Overwrite only the vintages in this run, never the whole table
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    bronze, silver = get_paths()
    orig_columns, perf_columns = load_column_names()

    print("Step 1/4: reading Bronze from", bronze)
    orig_raw = read_bronze(spark, f"{bronze}/origination", orig_columns)
    perf_raw = read_bronze(spark, f"{bronze}/performance", perf_columns)

    print("Step 2/4: cleaning")
    orig, orig_bad_values = clean_origination(orig_raw)
    perf, perf_bad_values = clean_performance(perf_raw)

    print("Step 3/4: quality checks")
    run_checks(orig, perf, orig_bad_values, perf_bad_values)
    orig_counts = rows_per_vintage(orig)
    perf_counts = rows_per_vintage(perf)

    print("Step 4/4: writing Silver to", silver)
    write_silver(orig, f"{silver}/origination")
    write_silver(perf, f"{silver}/performance")
    confirm_written(spark, f"{silver}/origination", orig_counts)
    confirm_written(spark, f"{silver}/performance", perf_counts)

    print("Freddie Mac Silver complete")
    print("Origination rows per vintage:", dict(sorted(orig_counts.items())))
    print("Performance rows per vintage:", dict(sorted(perf_counts.items())))


if __name__ == "__main__":
    main()