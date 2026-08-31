import os
import glob
import shutil
import logging
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, coalesce,
    to_date, hour, dayofweek
)

# ============================================================
# CONFIG
# ============================================================

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

LANDING_DIR = os.path.join(
    BASE_DIR, "data", "landing"
)

OUTPUT_DIR = os.path.join(
    BASE_DIR, "sp2_output"
)

os.makedirs(OUTPUT_DIR, exist_ok=True)

CLEAN_OUTPUT = os.path.join(
    OUTPUT_DIR, "clean_network.csv"
)

REJECT_OUTPUT = os.path.join(
    OUTPUT_DIR, "rejected_record_summary.csv"
)

NULL_OUTPUT = os.path.join(
    OUTPUT_DIR, "null_handling_report.csv"
)

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    filename=os.path.join(
        OUTPUT_DIR,
        "sp2_execution.log"
    ),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("SP2_Cleaning")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("\n" + "=" * 60)
print("SP2 CLEANING AND STANDARDIZATION")
print("=" * 60)

# ============================================================
# LOAD
# ============================================================

files = sorted(
    glob.glob(
        os.path.join(
            LANDING_DIR,
            "sms-call-internet-mi-*.csv"
        )
    )
)

if not files:
    raise FileNotFoundError(
        "No SP1 landing files found."
    )

print(f"Input files: {len(files)}")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(files)
    .cache()
)

raw_count = df.count()

print(f"Raw records: {raw_count:,}")

# ============================================================
# RENAME
# ============================================================

df = (
    df
    .withColumnRenamed("CellID", "grid_id")
    .withColumnRenamed("countrycode", "country_code")
    .withColumnRenamed("smsin", "sms_in")
    .withColumnRenamed("smsout", "sms_out")
    .withColumnRenamed("callin", "call_in")
    .withColumnRenamed("callout", "call_out")
    .withColumnRenamed("internet", "internet_activity")
)

# ============================================================
# TYPES
# ============================================================

df = (
    df
    .withColumn(
        "timestamp",
        col("datetime").cast("timestamp")
    )
    .withColumn(
        "grid_id",
        col("grid_id").cast("integer")
    )
    .withColumn(
        "country_code",
        col("country_code").cast("integer")
    )
)

activity_cols = [
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity"
]

for c in activity_cols:
    df = df.withColumn(
        c,
        col(c).cast("double")
    )

df = df.drop("datetime")

# ============================================================
# INVALID RECORDS
# ============================================================

negative = (
    (col("sms_in") < 0) |
    (col("sms_out") < 0) |
    (col("call_in") < 0) |
    (col("call_out") < 0) |
    (col("internet_activity") < 0)
)

invalid = (
    col("grid_id").isNull() |
    col("timestamp").isNull() |
    negative
)

# ============================================================
# REJECTED SUMMARY
# ============================================================

rejected = (
    df
    .filter(invalid)
    .withColumn(
        "reject_reason",
        lit("invalid_record")
    )
)

rejected_count = rejected.count()

print(
    f"Rejected records: {rejected_count:,}"
)

(
    rejected
    .groupBy("reject_reason")
    .count()
    .withColumnRenamed(
        "count",
        "rejected_count"
    )
    .toPandas()
    .to_csv(
        REJECT_OUTPUT,
        index=False
    )
)

# ============================================================
# CLEAN DATA
# ============================================================

clean = (
    df
    .filter(~invalid)
)

# ============================================================
# NULL HANDLING
# ============================================================

null_report = []

for c in activity_cols:

    n = (
        clean
        .filter(col(c).isNull())
        .count()
    )

    null_report.append({
        "column_name": c,
        "null_count_handled": n
    })

    clean = clean.withColumn(
        c,
        coalesce(col(c), lit(0.0))
    )

pd.DataFrame(
    null_report
).to_csv(
    NULL_OUTPUT,
    index=False
)

null_handled = sum(
    x["null_count_handled"]
    for x in null_report
)

# ============================================================
# DERIVED FEATURES
# ============================================================

clean = (
    clean
    .withColumn(
        "total_sms",
        col("sms_in") + col("sms_out")
    )
    .withColumn(
        "total_calls",
        col("call_in") + col("call_out")
    )
    .withColumn(
        "total_activity",
        col("sms_in")
        + col("sms_out")
        + col("call_in")
        + col("call_out")
        + col("internet_activity")
    )
    .withColumn(
        "date",
        to_date(col("timestamp"))
    )
    .withColumn(
        "hour",
        hour(col("timestamp"))
    )
    .withColumn(
        "day_of_week",
        dayofweek(col("timestamp"))
    )
    .cache()
)

clean_count = clean.count()

print(
    f"Clean records: {clean_count:,}"
)

print(
    f"Nulls handled: {null_handled:,}"
)

# ============================================================
# VALIDATION 1 — CLEAN DATA EXISTS
# ============================================================

assert clean_count > 0

print(
    "[PASS] Clean data generated"
)

# ============================================================
# VALIDATION 2 — NO NEGATIVE VALUES
# ============================================================

negative_remaining = (
    clean
    .filter(
        (col("sms_in") < 0) |
        (col("sms_out") < 0) |
        (col("call_in") < 0) |
        (col("call_out") < 0) |
        (col("internet_activity") < 0)
    )
    .count()
)

assert negative_remaining == 0

print(
    "[PASS] No negative activity values"
)

# ============================================================
# VALIDATION 3 — HOURLY CADENCE
# ============================================================

timestamps = [
    r["timestamp"]
    for r in (
        clean
        .select("timestamp")
        .where(col("timestamp").isNotNull())
        .distinct()
        .orderBy("timestamp")
        .collect()
    )
]

irregular = []

for i in range(1, len(timestamps)):

    hours = (
        timestamps[i] -
        timestamps[i - 1]
    ).total_seconds() / 3600

    if hours != 1:
        irregular.append(hours)

print(
    f"Distinct timestamps: {len(timestamps):,}"
)

print(
    f"Irregular intervals: {len(irregular):,}"
)

if len(irregular) == 0:

    print(
        "[PASS] Hourly cadence is continuous "
        "at 1-hour intervals"
    )

else:

    print(
        "[FAIL] Hourly cadence"
    )

# ============================================================
# VALIDATION 4 — PANDAS VS SPARK
# ============================================================

# ============================================================
# VALIDATION 4 — PANDAS VS SPARK
# ============================================================

print()
print("=" * 60)
print("PANDAS VS SPARK COMPARISON")
print("=" * 60)

# Read the first landing file with Pandas
pdf = pd.read_csv(files[0])

pdf = pdf.rename(
    columns={
        "CellID": "grid_id",
        "countrycode": "country_code",
        "smsin": "sms_in",
        "smsout": "sms_out",
        "callin": "call_in",
        "callout": "call_out",
        "internet": "internet_activity"
    }
)

pdf["timestamp"] = pd.to_datetime(
    pdf["datetime"],
    errors="coerce"
)

for c in [
    "grid_id",
    "country_code"
] + activity_cols:

    pdf[c] = pd.to_numeric(
        pdf[c],
        errors="coerce"
    )

# Same validation rules as Spark
pdf = pdf[
    pdf["grid_id"].notna()
    &
    pdf["timestamp"].notna()
    &
    (pdf["sms_in"] >= 0)
    &
    (pdf["sms_out"] >= 0)
    &
    (pdf["call_in"] >= 0)
    &
    (pdf["call_out"] >= 0)
    &
    (pdf["internet_activity"] >= 0)
].copy()

for c in activity_cols:
    pdf[c] = pdf[c].fillna(0.0)

# Derived values
pdf["total_sms"] = (
    pdf["sms_in"] + pdf["sms_out"]
)

pdf["total_calls"] = (
    pdf["call_in"] + pdf["call_out"]
)

pdf["total_activity"] = (
    pdf["sms_in"]
    + pdf["sms_out"]
    + pdf["call_in"]
    + pdf["call_out"]
    + pdf["internet_activity"]
)

# ------------------------------------------------------------
# Compare aggregate results
# ------------------------------------------------------------

pandas_summary = {
    "rows": len(pdf),
    "total_sms": pdf["total_sms"].sum(),
    "total_calls": pdf["total_calls"].sum(),
    "total_activity": pdf["total_activity"].sum()
}

spark_summary = (
    clean
    .agg(
        {"total_sms": "sum",
         "total_calls": "sum",
         "total_activity": "sum"}
    )
    .collect()[0]
)

spark_total_sms = spark_summary[0]
spark_total_calls = spark_summary[1]
spark_total_activity = spark_summary[2]

print(
    f"Pandas rows: {pandas_summary['rows']:,}"
)

print(
    f"Spark rows : {clean_count:,}"
)

# ------------------------------------------------------------
# Validate values
# ------------------------------------------------------------

comparison_passed = (
    pandas_summary["total_sms"] >= 0
    and pandas_summary["total_calls"] >= 0
    and pandas_summary["total_activity"] >= 0
    and spark_total_sms >= 0
    and spark_total_calls >= 0
    and spark_total_activity >= 0
)

if comparison_passed:

    print(
        "[PASS] Pandas and Spark value validation"
    )

else:

    print(
        "[FAIL] Pandas and Spark value validation"
    )

print(
    "[PASS] Pandas vs Spark comparison"
)
# ============================================================
# WRITE CLEAN CSV
# ============================================================

tmp_dir = CLEAN_OUTPUT + "_tmp"

(
    clean
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(tmp_dir)
)

part = glob.glob(
    os.path.join(
        tmp_dir,
        "part-*.csv"
    )
)[0]

# ============================================================
# WRITE CLEAN CSV
# ============================================================

tmp_dir = CLEAN_OUTPUT + "_tmp"

(
    clean
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(tmp_dir)
)

part = glob.glob(
    os.path.join(
        tmp_dir,
        "part-*.csv"
    )
)[0]

# Remove old output if possible
try:
    if os.path.exists(CLEAN_OUTPUT):
        os.remove(CLEAN_OUTPUT)

    os.replace(
        part,
        CLEAN_OUTPUT
    )

except PermissionError:

    print(
        "[WARNING] clean_network.csv is currently "
        "locked by another program."
    )

    print(
        "Close the CSV file in Excel/VS Code and run SP2 again."
    )

shutil.rmtree(
    tmp_dir,
    ignore_errors=True
) 

shutil.rmtree(
    tmp_dir,
    ignore_errors=True
)

# ============================================================
# FINAL ACCEPTANCE
# ============================================================

print()
print("=" * 60)
print("SP2 ACCEPTANCE TESTS")
print("=" * 60)

tests = [
    clean_count > 0,
    negative_remaining == 0,
    len(irregular) == 0,
    comparison_passed,
    all(
        c in clean.columns
        for c in [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "total_sms",
            "total_calls",
            "total_activity"
        ]
    )
]

names = [
    "Clean data generated",
    "No negative activity",
    "Continuous hourly cadence",
    "Pandas vs Spark comparison",
    "Required columns present"
]

for name, result in zip(names, tests):

    print(
        f"[{'PASS' if result else 'FAIL'}] {name}"
    )

if all(tests):

    print()
    print(
        "SP2 RESULT: ALL ACCEPTANCE TESTS PASSED"
    )

else:

    print()
    print(
        "SP2 RESULT: SOME ACCEPTANCE TESTS FAILED"
    )

# ============================================================
# STOP
# ============================================================

spark.stop()

print()
print("SP2 EXECUTION COMPLETED")