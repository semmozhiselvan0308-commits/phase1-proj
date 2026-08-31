import os
import sys
import glob

# ============================================================
# PYSPARK WINDOWS / PYTHON CONFIGURATION
# ============================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    IntegerType,
    DoubleType
)
from pyspark.sql.functions import (
    input_file_name,
    col,
    count,
    countDistinct,
    min,
    max,
    date_trunc,
    when
)


# ============================================================
# SP1 — DISTRIBUTED INGESTION
# ============================================================

DATA_FOLDER = "../data/landing"
FILE_PATTERN = "sms-call-internet-mi-*.csv"


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("SP1_Distributed_Ingestion")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("\n" + "=" * 70)
print("SP1 — DISTRIBUTED INGESTION")
print("=" * 70)


# ============================================================
# 2. MANUAL SCHEMA
# ============================================================

schema = StructType([
    StructField("datetime", TimestampType(), True),
    StructField("CellID", IntegerType(), True),
    StructField("countrycode", IntegerType(), True),
    StructField("smsin", DoubleType(), True),
    StructField("smsout", DoubleType(), True),
    StructField("callin", DoubleType(), True),
    StructField("callout", DoubleType(), True),
    StructField("internet", DoubleType(), True)
])


print("\nManual schema created.")


# ============================================================
# 3. FIND ACTUAL FILES IN LANDING FOLDER
# ============================================================

script_directory = os.path.dirname(os.path.abspath(__file__))

absolute_data_folder = os.path.abspath(
    os.path.join(script_directory, DATA_FOLDER)
)

file_pattern = os.path.join(
    absolute_data_folder,
    FILE_PATTERN
)

actual_files = sorted(glob.glob(file_pattern))

actual_file_count = len(actual_files)


print("\n" + "=" * 70)
print("FILES FOUND")
print("=" * 70)

for file in actual_files:
    print(os.path.basename(file))

print(f"\nActual file count: {actual_file_count}")


# ============================================================
# 4. VALIDATE FILE EXISTENCE
# ============================================================

if actual_file_count == 0:
    raise FileNotFoundError(
        f"No files found using pattern:\n{file_pattern}"
    )


# ============================================================
# 5. READ ALL DAILY FILES USING SPARK
# ============================================================
# Use the exact files found by Python glob.
# This avoids the Windows Hadoop wildcard/globbing problem.

raw_network_df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv(actual_files)
)


# ============================================================
# 6. ADD SOURCE FILE NAME
# ============================================================

raw_network_df = raw_network_df.withColumn(
    "input_file_name",
    input_file_name()
)


# ============================================================
# 7. DISPLAY VALIDATED SCHEMA
# ============================================================

print("\n" + "=" * 70)
print("VALIDATED SCHEMA")
print("=" * 70)

raw_network_df.printSchema()


# ============================================================
# 8. BASIC ROW COUNT
# ============================================================

row_count = raw_network_df.count()


# ============================================================
# 9. IDENTIFY COMPLETELY BLANK ROWS
# ============================================================
# The source contains one completely empty data row.
# It should not be treated as a real invalid CellID record.

blank_row_condition = (
    col("CellID").isNull() &
    col("countrycode").isNull() &
    col("smsin").isNull() &
    col("smsout").isNull() &
    col("callin").isNull() &
    col("callout").isNull() &
    col("internet").isNull()
)

blank_row_count = (
    raw_network_df
    .filter(blank_row_condition)
    .count()
)


print(f"\nCompletely blank source rows : {blank_row_count}")


# ============================================================
# 10. UNIQUE CELL COUNT
# ============================================================

unique_cells = (
    raw_network_df
    .select(countDistinct("CellID"))
    .collect()[0][0]
)


# ============================================================
# 11. COUNTRY-CODE CATEGORY COUNT
# ============================================================

country_code_categories = (
    raw_network_df
    .select(countDistinct("countrycode"))
    .collect()[0][0]
)


# ============================================================
# 12. DISTINCT HOURLY INTERVAL COUNT
# ============================================================

hourly_df = raw_network_df.withColumn(
    "hour",
    date_trunc("hour", col("datetime"))
)

distinct_hourly_intervals = (
    hourly_df
    .filter(col("datetime").isNotNull())
    .select("hour")
    .distinct()
    .count()
)


# ============================================================
# 13. PARTITION COUNT
# ============================================================

partition_count = raw_network_df.rdd.getNumPartitions()


# ============================================================
# 14. DISPLAY INGESTION METRICS
# ============================================================

print("\n" + "=" * 70)
print("SP1 INGESTION METRICS")
print("=" * 70)

print(f"Total rows                  : {row_count}")
print(f"Source files                : {actual_file_count}")
print(f"Unique CellID values        : {unique_cells}")
print(f"Country-code categories     : {country_code_categories}")
print(f"Distinct hourly intervals   : {distinct_hourly_intervals}")
print(f"Spark partitions            : {partition_count}")


# ============================================================
# 15. FILE-LEVEL ROW COUNT REPORT
# ============================================================

print("\n" + "=" * 70)
print("FILE-LEVEL ROW COUNT REPORT")
print("=" * 70)

file_counts = (
    raw_network_df
    .groupBy("input_file_name")
    .agg(count("*").alias("row_count"))
    .orderBy("input_file_name")
)

file_counts.show(
    n=actual_file_count,
    truncate=False
)


# ============================================================
# 16. CELLID RANGE
# ============================================================

cell_range = (
    raw_network_df
    .select(
        min("CellID").alias("min_cell"),
        max("CellID").alias("max_cell")
    )
    .collect()[0]
)

min_cell = cell_range["min_cell"]
max_cell = cell_range["max_cell"]


print("\n" + "=" * 70)
print("CELLID RANGE")
print("=" * 70)

print(f"Minimum CellID: {min_cell}")
print(f"Maximum CellID: {max_cell}")


# ============================================================
# 17. CHECK SOURCE FILE TRACEABILITY
# ============================================================

null_filename_count = (
    raw_network_df
    .filter(col("input_file_name").isNull())
    .count()
)

dataframe_file_count = (
    raw_network_df
    .select(countDistinct("input_file_name"))
    .collect()[0][0]
)


print("\n" + "=" * 70)
print("TRACEABILITY")
print("=" * 70)

print(f"Files represented in DataFrame : {dataframe_file_count}")
print(f"Rows with missing filename      : {null_filename_count}")


# ============================================================
# 18. CHECK FOR INVALID CELL IDs
# ============================================================
# Completely blank rows are excluded.
#
# A CellID is invalid only when a non-blank source record
# contains NULL, < 1, or > 10000.

invalid_cell_count = (
    raw_network_df
    .filter(
        ~blank_row_condition &
        (
            col("CellID").isNull() |
            (col("CellID") < 1) |
            (col("CellID") > 10000)
        )
    )
    .count()
)


print(f"Invalid CellID rows             : {invalid_cell_count}")


# ============================================================
# 19. CHECK RAW GRAIN
# ============================================================

hourly_cell_count = (
    raw_network_df
    .filter(
        col("CellID").isNotNull() &
        col("datetime").isNotNull()
    )
    .select(
        "CellID",
        date_trunc("hour", col("datetime")).alias("hour")
    )
    .distinct()
    .count()
)


print("\n" + "=" * 70)
print("RAW GRAIN CHECK")
print("=" * 70)

print(f"Raw country-code rows       : {row_count}")
print(f"Distinct CellID-hour rows   : {hourly_cell_count}")


# ============================================================
# 20. SOURCE HOURLY COVERAGE CHECK
# ============================================================
# Instead of assuming D × 24, determine how many distinct
# hourly intervals actually exist in the source data.
#
# The Spark DataFrame is then checked to make sure every
# source hour is represented after ingestion.

source_hourly_intervals = (
    raw_network_df
    .filter(col("datetime").isNotNull())
    .select(
        date_trunc("hour", col("datetime")).alias("hour")
    )
    .distinct()
    .count()
)

ingested_hourly_intervals = (
    hourly_df
    .filter(col("datetime").isNotNull())
    .select("hour")
    .distinct()
    .count()
)

hourly_coverage_valid = (
    ingested_hourly_intervals == source_hourly_intervals
)


# ============================================================
# 21. ACCEPTANCE TESTS
# ============================================================

print("\n" + "=" * 70)
print("SP1 ACCEPTANCE TESTS")
print("=" * 70)


# ------------------------------------------------------------
# TEST 1
# All hourly intervals present in source are ingested
# ------------------------------------------------------------

test_1 = hourly_coverage_valid

print(
    f"[{'PASS' if test_1 else 'FAIL'}] "
    f"All source hourly intervals successfully ingested "
    f"({ingested_hourly_intervals} == {source_hourly_intervals})"
)


# ------------------------------------------------------------
# TEST 2
# File count in DataFrame = actual files in folder
# ------------------------------------------------------------

test_2 = (
    dataframe_file_count == actual_file_count
)

print(
    f"[{'PASS' if test_2 else 'FAIL'}] "
    f"File count matches actual folder files "
    f"({dataframe_file_count} == {actual_file_count})"
)


# ------------------------------------------------------------
# TEST 3
# All NON-BLANK CellID values between 1 and 10000
# ------------------------------------------------------------

test_3 = (
    invalid_cell_count == 0
)

print(
    f"[{'PASS' if test_3 else 'FAIL'}] "
    f"All non-blank CellID values fall within 1–10000"
)


# ------------------------------------------------------------
# TEST 4
# Raw country-code grain preserved
# ------------------------------------------------------------

test_4 = (
    row_count > hourly_cell_count
)

print(
    f"[{'PASS' if test_4 else 'FAIL'}] "
    f"Raw row count > hourly CellID count "
    f"({row_count} > {hourly_cell_count})"
)


# ------------------------------------------------------------
# TEST 5
# Every row has source filename
# ------------------------------------------------------------

test_5 = (
    null_filename_count == 0
)

print(
    f"[{'PASS' if test_5 else 'FAIL'}] "
    f"Every row has input_file_name"
)


# ============================================================
# 22. FINAL ACCEPTANCE RESULT
# ============================================================

all_tests_pass = all([
    test_1,
    test_2,
    test_3,
    test_4,
    test_5
])


print("\n" + "=" * 70)

if all_tests_pass:
    print("SP1 RESULT: ALL ACCEPTANCE TESTS PASSED")
else:
    print("SP1 RESULT: ONE OR MORE ACCEPTANCE TESTS FAILED")

print("=" * 70)


# ============================================================
# 23. STOP SPARK
# ============================================================

spark.stop()

print("\nSP1 execution completed.")
