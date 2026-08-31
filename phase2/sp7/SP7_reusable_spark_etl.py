"""
SP7 - Reusable Spark ETL Job

Purpose:
    Build a reusable Spark ETL pipeline that processes
    all supplied daily telecom activity CSV files.

Pipeline:
    1. Discover daily input files
    2. Read raw CSV files using Spark
    3. Standardize column names
    4. Cast data types
    5. Handle invalid / rejected records
    6. Aggregate activity by timestamp + grid_id
    7. Produce hourly grid analytics
    8. Write cleaned activity as partitioned Parquet
    9. Write hourly analytics as Parquet
    10. Create dashboard summary CSV
    11. Validate outputs
    12. Support overwrite / append modes
    13. Log execution details
"""

import os
import csv
import shutil
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    hour,
    coalesce,
    lit,
    sum as spark_sum,
    countDistinct,
    min as spark_min,
    max as spark_max,
    when
)


# ============================================================
# PATH CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = BASE_DIR / "data"

LANDING_DIR = DATA_DIR / "landing"
REFERENCE_DIR = DATA_DIR / "reference"

PROCESSED_DIR = DATA_DIR / "processed"
ANALYTICS_DIR = DATA_DIR / "analytics"

ACTIVITY_PARQUET = PROCESSED_DIR / "activity"
HOURLY_PARQUET = ANALYTICS_DIR / "hourly_grid_summary"

DASHBOARD_CSV = DATA_DIR / "dashboard_summary.csv"

SPARK_WAREHOUSE = DATA_DIR / "spark_warehouse"

LOG_FILE = BASE_DIR / "sp7_execution.log"

# GeoJSON reference file
GEOJSON_FILE = REFERENCE_DIR / "milano-grid.geojson"


# ============================================================
# CONFIGURATION
# ============================================================

INPUT_PATTERN = "sms-call-internet-mi-*.csv"

# overwrite = rebuild complete dataset
# append    = add new data without deleting existing data
WRITE_MODE = "overwrite"

EXPECTED_INPUT_FILES = 7


# ============================================================
# DIRECTORY CREATION
# ============================================================

LANDING_DIR.mkdir(parents=True, exist_ok=True)
REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
SPARK_WAREHOUSE.mkdir(parents=True, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("SP7")


# ============================================================
# WINDOWS HADOOP CONFIGURATION
# ============================================================

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def delete_directory(path):
    """Remove an existing output directory."""
    if path.exists():
        shutil.rmtree(path)
        logger.info(f"Removed existing directory: {path}")


def directory_size(path):
    """Return total size of files inside a directory."""
    if not path.exists():
        return 0

    total = 0

    for root, dirs, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)

            try:
                total += os.path.getsize(file_path)
            except OSError:
                pass

    return total


def format_bytes(size):
    """Convert bytes to readable format."""

    if size < 1024:
        return f"{size} B"

    if size < 1024 ** 2:
        return f"{size / 1024:.2f} KB"

    if size < 1024 ** 3:
        return f"{size / (1024 ** 2):.2f} MB"

    return f"{size / (1024 ** 3):.2f} GB"


def get_partition_folders(path):
    """Return date partition folders."""

    if not path.exists():
        return []

    return sorted(
        item.name
        for item in path.iterdir()
        if item.is_dir()
        and item.name.startswith("date=")
    )


def discover_input_files():
    """Discover all daily telecom CSV files."""

    return sorted(
        LANDING_DIR.glob(INPUT_PATTERN)
    )


# ============================================================
# SPARK SESSION
# ============================================================

spark = None

try:

    spark = (
        SparkSession.builder
        .appName("SP7_Reusable_Spark_ETL")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config(
            "spark.sql.warehouse.dir",
            str(SPARK_WAREHOUSE)
        )
        .config(
            "spark.hadoop.fs.file.impl",
            "org.apache.hadoop.fs.LocalFileSystem"
        )
        .config(
            "spark.hadoop.io.native.lib.available",
            "false"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")


    # ========================================================
    # START
    # ========================================================

    print("=" * 70)
    print("SP7 - REUSABLE SPARK ETL JOB")
    print("=" * 70)

    logger.info("=" * 70)
    logger.info("SP7 START")
    logger.info("=" * 70)


    # ========================================================
    # 1. DISCOVER INPUT FILES
    # ========================================================

    print("\n[1] Discovering input files...")

    input_files = discover_input_files()

    print(f"Input directory: {LANDING_DIR}")
    print(f"Pattern: {INPUT_PATTERN}")
    print(f"Files found: {len(input_files)}")

    for file in input_files:
        print(f"  {file.name}")

    logger.info(
        f"Input files discovered: {len(input_files)}"
    )

    if len(input_files) == 0:
        raise FileNotFoundError(
            f"No input files found in {LANDING_DIR}"
        )

    if len(input_files) != EXPECTED_INPUT_FILES:
        print(
            f"WARNING: Expected approximately "
            f"{EXPECTED_INPUT_FILES} files, "
            f"but found {len(input_files)}."
        )

        logger.warning(
            f"Expected {EXPECTED_INPUT_FILES} files, "
            f"found {len(input_files)}"
        )

    print("PASS: Input discovery completed.")


    # ========================================================
    # 2. READ RAW DATA
    # ========================================================

    print("\n[2] Reading raw CSV data...")

    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(
            [str(file) for file in input_files]
        )
    )

    raw_count = raw_df.count()

    print(f"Raw records: {raw_count:,}")

    print("\nRaw schema:")
    raw_df.printSchema()

    logger.info(
        f"Raw records: {raw_count}"
    )


    # ========================================================
    # 3. STANDARDIZE COLUMN NAMES
    # ========================================================

    print("\n[3] Standardizing column names...")

    rename_map = {
        "datetime": "timestamp",
        "CellID": "grid_id",
        "countrycode": "country_code",
        "smsin": "sms_in",
        "smsout": "sms_out",
        "callin": "call_in",
        "callout": "call_out",
        "internet": "internet_activity"
    }

    standardized_df = raw_df

    for old_name, new_name in rename_map.items():

        if old_name in standardized_df.columns:

            standardized_df = (
                standardized_df
                .withColumnRenamed(
                    old_name,
                    new_name
                )
            )

    required_columns = [
        "timestamp",
        "grid_id",
        "country_code",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity"
    ]

    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in standardized_df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: "
            f"{missing_columns}"
        )

    print(
        "PASS: Column standardization completed."
    )


    # ========================================================
    # 4. CAST DATA TYPES
    # ========================================================

    print("\n[4] Casting data types...")

    clean_df = (
        standardized_df
        .withColumn(
            "timestamp",
            to_timestamp(col("timestamp"))
        )
        .withColumn(
            "grid_id",
            col("grid_id").cast("integer")
        )
        .withColumn(
            "country_code",
            col("country_code").cast("integer")
        )
        .withColumn(
            "sms_in",
            col("sms_in").cast("double")
        )
        .withColumn(
            "sms_out",
            col("sms_out").cast("double")
        )
        .withColumn(
            "call_in",
            col("call_in").cast("double")
        )
        .withColumn(
            "call_out",
            col("call_out").cast("double")
        )
        .withColumn(
            "internet_activity",
            col("internet_activity").cast("double")
        )
    )

    print(
        "PASS: Data types cast successfully."
    )


    # ========================================================
    # 5. REJECT INVALID RECORDS
    # ========================================================

    print("\n[5] Validating records...")

    invalid_df = clean_df.filter(
        col("timestamp").isNull()
        |
        col("grid_id").isNull()
    )

    invalid_count = invalid_df.count()

    print(
        f"Rejected records: {invalid_count:,}"
    )

    if invalid_count > 0:

        print(
            "WARNING: Invalid records detected."
        )

        logger.warning(
            f"Rejected records: {invalid_count}"
        )

    valid_df = clean_df.filter(
        col("timestamp").isNotNull()
        &
        col("grid_id").isNotNull()
    )

    valid_count = valid_df.count()

    print(
        f"Valid records: {valid_count:,}"
    )

    if valid_count == 0:
        raise ValueError(
            "No valid records remain after validation."
        )

    print(
        "PASS: Record validation completed."
    )


    # ========================================================
    # 6. HANDLE NULL ACTIVITY VALUES
    # ========================================================

    print(
        "\n[6] Handling activity null values..."
    )

    activity_columns = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity"
    ]

    for column_name in activity_columns:

        valid_df = valid_df.withColumn(
            column_name,
            coalesce(
                col(column_name),
                lit(0.0)
            )
        )

    print(
        "PASS: Null activity values replaced with 0."
    )


    # ========================================================
    # 7. ADD DATE AND HOUR
    # ========================================================

    print("\n[7] Creating time features...")

    valid_df = (
        valid_df
        .withColumn(
            "date",
            to_date(col("timestamp"))
        )
        .withColumn(
            "hour",
            hour(col("timestamp"))
        )
    )

    null_date_count = (
        valid_df
        .filter(col("date").isNull())
        .count()
    )

    if null_date_count > 0:
        raise AssertionError(
            f"Found {null_date_count} NULL dates."
        )

    print(
        "PASS: Date and hour features created."
    )


    # ========================================================
    # 8. WRITE CLEAN ACTIVITY PARQUET
    # ========================================================

    print(
        "\n[8] Writing clean activity Parquet..."
    )

    if WRITE_MODE == "overwrite":
        delete_directory(ACTIVITY_PARQUET)

    (
        valid_df
        .select(
            "timestamp",
            "grid_id",
            "country_code",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "date"
        )
        .repartition("date")
        .write
        .mode(WRITE_MODE)
        .partitionBy("date")
        .parquet(
            str(ACTIVITY_PARQUET)
        )
    )

    print(
        "PASS: Activity Parquet written to:"
    )

    print(
        f"  {ACTIVITY_PARQUET}"
    )

    activity_partitions = (
        get_partition_folders(
            ACTIVITY_PARQUET
        )
    )

    print(
        f"Date partitions: "
        f"{len(activity_partitions)}"
    )


    # ========================================================
    # 9. CREATE HOURLY GRID AGGREGATION
    # ========================================================

    print(
        "\n[9] Creating hourly grid analytics..."
    )

    hourly_df = (
        valid_df
        .groupBy(
            "timestamp",
            "grid_id"
        )
        .agg(
            spark_sum("sms_in")
            .alias("sms_in"),

            spark_sum("sms_out")
            .alias("sms_out"),

            spark_sum("call_in")
            .alias("call_in"),

            spark_sum("call_out")
            .alias("call_out"),

            spark_sum(
                "internet_activity"
            ).alias(
                "internet_activity"
            )
        )
        .withColumn(
            "total_sms_activity",
            col("sms_in")
            + col("sms_out")
        )
        .withColumn(
            "total_call_activity",
            col("call_in")
            + col("call_out")
        )
        .withColumn(
            "total_activity",
            col("total_sms_activity")
            + col("total_call_activity")
            + col("internet_activity")
        )
        .withColumn(
            "internet_share",
            when(
                col("total_activity") > 0,
                col("internet_activity")
                / col("total_activity")
            ).otherwise(0.0)
        )
    )

    hourly_df = hourly_df.select(
        "timestamp",
        "grid_id",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "total_sms_activity",
        "total_call_activity",
        "internet_activity",
        "total_activity",
        "internet_share"
    )

    hourly_count = hourly_df.count()

    print(
        f"Hourly analytics rows: "
        f"{hourly_count:,}"
    )


    # ========================================================
    # 10. DUPLICATE VALIDATION
    # ========================================================

    print(
        "\n[10] Checking duplicate grid/hour records..."
    )

    duplicate_count = (
        hourly_df
        .groupBy(
            "grid_id",
            "timestamp"
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    print(
        f"Duplicate groups: "
        f"{duplicate_count}"
    )

    if duplicate_count != 0:
        raise AssertionError(
            "Duplicate (grid_id, timestamp) "
            "records found."
        )

    print(
        "PASS: One record per grid and timestamp."
    )


    # ========================================================
    # 11. GEOMETRY VALIDATION
    # ========================================================

    print(
        "\n[11] Checking geometry separation..."
    )

    geometry_columns = [
        column_name
        for column_name in hourly_df.columns
        if column_name.lower()
        in [
            "geometry",
            "geom",
            "polygon"
        ]
    ]

    if geometry_columns:
        raise AssertionError(
            f"Geometry columns found: "
            f"{geometry_columns}"
        )

    print(
        "PASS: No geometry in hourly analytics."
    )


    # ========================================================
    # 12. WRITE HOURLY PARQUET
    # ========================================================

    print(
        "\n[12] Writing hourly analytics Parquet..."
    )

    if WRITE_MODE == "overwrite":
        delete_directory(HOURLY_PARQUET)

    (
        hourly_df
        .write
        .mode(WRITE_MODE)
        .parquet(
            str(HOURLY_PARQUET)
        )
    )

    print(
        "PASS: Hourly Parquet written to:"
    )

    print(
        f"  {HOURLY_PARQUET}"
    )


    # ========================================================
    # 13. ROUND-TRIP VALIDATION
    # ========================================================

    print(
        "\n[13] Running round-trip validation..."
    )

    activity_roundtrip = (
        spark.read
        .parquet(
            str(ACTIVITY_PARQUET)
        )
    )

    hourly_roundtrip = (
        spark.read
        .parquet(
            str(HOURLY_PARQUET)
        )
    )

    activity_roundtrip_count = (
        activity_roundtrip.count()
    )

    hourly_roundtrip_count = (
        hourly_roundtrip.count()
    )

    print(
        f"Activity original rows: "
        f"{valid_count:,}"
    )

    print(
        f"Activity round-trip rows: "
        f"{activity_roundtrip_count:,}"
    )

    print(
        f"Hourly original rows: "
        f"{hourly_count:,}"
    )

    print(
        f"Hourly round-trip rows: "
        f"{hourly_roundtrip_count:,}"
    )

    if valid_count != activity_roundtrip_count:
        raise AssertionError(
            "Activity round-trip row count mismatch."
        )

    if hourly_count != hourly_roundtrip_count:
        raise AssertionError(
            "Hourly round-trip row count mismatch."
        )

    print(
        "PASS: Round-trip validation completed."
    )


    # ========================================================
    # 14. ROUND-TRIP DUPLICATE CHECK
    # ========================================================

    print(
        "\n[14] Checking duplicates after round trip..."
    )

    roundtrip_duplicates = (
        hourly_roundtrip
        .groupBy(
            "grid_id",
            "timestamp"
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    if roundtrip_duplicates != 0:
        raise AssertionError(
            "Duplicates found after round trip."
        )

    print(
        "PASS: No duplicates after round trip."
    )


    # ========================================================
    # 15. CREATE DASHBOARD SUMMARY
    # ========================================================

    print(
        "\n[15] Creating dashboard summary..."
    )

    distinct_grids = (
        hourly_df
        .select(
            countDistinct("grid_id")
        )
        .collect()[0][0]
    )

    start_timestamp = (
        hourly_df
        .select(
            spark_min("timestamp")
        )
        .collect()[0][0]
    )

    end_timestamp = (
        hourly_df
        .select(
            spark_max("timestamp")
        )
        .collect()[0][0]
    )

    total_activity = (
        hourly_df
        .select(
            spark_sum("total_activity")
        )
        .collect()[0][0]
    )

    total_internet = (
        hourly_df
        .select(
            spark_sum("internet_activity")
        )
        .collect()[0][0]
    )

    dashboard_rows = [
        ["metric", "value"],

        [
            "input_files",
            len(input_files)
        ],

        [
            "raw_activity_rows",
            raw_count
        ],

        [
            "valid_activity_rows",
            valid_count
        ],

        [
            "rejected_records",
            invalid_count
        ],

        [
            "hourly_grid_rows",
            hourly_count
        ],

        [
            "distinct_grids",
            distinct_grids
        ],

        [
            "start_timestamp",
            str(start_timestamp)
        ],

        [
            "end_timestamp",
            str(end_timestamp)
        ],

        [
            "total_activity",
            total_activity
        ],

        [
            "total_internet_activity",
            total_internet
        ],

        [
            "date_partitions",
            len(activity_partitions)
        ]
    ]

    if DASHBOARD_CSV.exists():
        DASHBOARD_CSV.unlink()

    with open(
        DASHBOARD_CSV,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        writer.writerows(
            dashboard_rows
        )

    print(
        "PASS: Dashboard summary created."
    )

    print(
        f"  {DASHBOARD_CSV}"
    )


    # ========================================================
    # 16. FILE SIZE COMPARISON
    # ========================================================

    print(
        "\n[16] File-size comparison..."
    )

    raw_csv_size = sum(
        file.stat().st_size
        for file in input_files
        if file.exists()
    )

    activity_parquet_size = (
        directory_size(
            ACTIVITY_PARQUET
        )
    )

    hourly_parquet_size = (
        directory_size(
            HOURLY_PARQUET
        )
    )

    print(
        f"Input CSV size: "
        f"{format_bytes(raw_csv_size)}"
    )

    print(
        f"Activity Parquet size: "
        f"{format_bytes(activity_parquet_size)}"
    )

    print(
        f"Hourly Parquet size: "
        f"{format_bytes(hourly_parquet_size)}"
    )

    if activity_parquet_size > 0:

        ratio = (
            raw_csv_size
            / activity_parquet_size
        )

        print(
            f"CSV / Activity Parquet ratio: "
            f"{ratio:.2f}x"
        )

    print(
        "PASS: File-size comparison completed."
    )


    # ========================================================
    # 17. REFERENCE GEOJSON VALIDATION
    # ========================================================

    print(
        "\n[17] Checking reference GeoJSON..."
    )

    geojson_exists = GEOJSON_FILE.exists()

    if geojson_exists:

        print(
            "PASS: GeoJSON retained separately."
        )

        print(
            f"  {GEOJSON_FILE}"
        )

    else:

        print(
            "WARNING: GeoJSON reference file not found."
        )

        print(
            "Expected location:"
        )

        print(
            f"  {GEOJSON_FILE}"
        )


    # ========================================================
    # 18. OUTPUT VALIDATION
    # ========================================================

    print(
        "\n[18] Final output validation..."
    )

    tests = {

        "Input files discovered":
            len(input_files) > 0,

        "Valid records exist":
            valid_count > 0,

        "Activity Parquet exists":
            ACTIVITY_PARQUET.exists(),

        "Activity date partitions exist":
            len(activity_partitions) > 0,

        "Hourly Parquet exists":
            HOURLY_PARQUET.exists(),

        "Dashboard CSV exists":
            DASHBOARD_CSV.exists(),

        "Activity round trip":
            valid_count
            == activity_roundtrip_count,

        "Hourly round trip":
            hourly_count
            == hourly_roundtrip_count,

        "No duplicate grid/hour records":
            duplicate_count == 0,

        "No duplicates after round trip":
            roundtrip_duplicates == 0,

        "No geometry in hourly analytics":
            len(geometry_columns) == 0,

        "GeoJSON retained separately":
            geojson_exists,

        "File size comparison":
            (
                raw_csv_size > 0
                and activity_parquet_size > 0
            )
    }

    all_passed = True

    for test_name, result in tests.items():

        if result:

            print(
                f"PASS: {test_name}"
            )

            logger.info(
                f"PASS: {test_name}"
            )

        else:

            print(
                f"FAIL: {test_name}"
            )

            logger.error(
                f"FAIL: {test_name}"
            )

            all_passed = False


    # ========================================================
    # 19. REUSABILITY INFORMATION
    # ========================================================

    print(
        "\n[19] Reusable ETL configuration"
    )

    print(
        f"Input pattern : {INPUT_PATTERN}"
    )

    print(
        f"Write mode    : {WRITE_MODE}"
    )

    print(
        f"Input path    : {LANDING_DIR}"
    )

    print(
        f"Output path   : {PROCESSED_DIR}"
    )

    print(
        """

Overwrite vs Append:

OVERWRITE:
    Used when the complete dataset is rebuilt from all
    supplied source files.

APPEND:
    Appropriate only when genuinely new data is being added
    and existing partitions must remain untouched.

WARNING:
    Blind APPEND can create duplicate records when the same
    daily files are processed repeatedly.

REUSABILITY:
    The ETL discovers files automatically using a pattern,
    so new daily files matching the pattern can be processed
    without changing the transformation logic.

"""
    )


    # ========================================================
    # 20. FINAL RESULT
    # ========================================================

    print(
        "\n" + "=" * 70
    )

    if all_passed:

        print(
            "SP7 COMPLETED SUCCESSFULLY"
        )

        logger.info(
            "SP7 COMPLETED SUCCESSFULLY"
        )

    else:

        print(
            "SP7 FAILED - CHECK THE TESTS ABOVE"
        )

        logger.error(
            "SP7 FAILED"
        )

    print(
        "=" * 70
    )


except Exception as error:

    print(
        "\n" + "=" * 70
    )

    print(
        "SP7 FAILED"
    )

    print(
        "=" * 70
    )

    print(
        f"Error: {error}"
    )

    try:

        logger.exception(
            "SP7 FAILED"
        )

    except Exception:
        pass

    raise


finally:

    # ========================================================
    # SAFE SPARK SHUTDOWN
    # ========================================================

    try:

        if spark is not None:
            spark.stop()

    except Exception:
        pass