
"""
SP6 - Write Processed & Analytics Data

Requirements:
1. Clean activity -> Parquet
2. Partition cleaned activity by date3
3. hourly_grid_summary -> Parquet
4. No geometry in hourly analytics
5. GeoJSON retained separately in data/reference/
6. Dashboard summary -> CSV
7. Round-trip validation
8. Duplicate validation
9. File-size comparison
10. Explain overwrite vs append
"""

import os
import shutil
import csv
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    countDistinct,
    min as spark_min,
    max as spark_max,
    sum as spark_sum
)


# ============================================================
# PATH CONFIGURATION
# ============================================================

# Script location:
# phase2/sp6/SP6_write_processed_analytics.py
#
# parents[0] = phase2/sp6
# parents[1] = phase2

BASE_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = BASE_DIR / "data"

# SP2 input
SP2_OUTPUT = BASE_DIR / "sp2" / "sp2_output"
CLEAN_ACTIVITY = SP2_OUTPUT / "clean_network.csv"

# SP3 input
SP3_DIR = BASE_DIR / "sp3"
HOURLY_SUMMARY = SP3_DIR / "hourly_grid_summary.csv"

# Reference data
REFERENCE_DIR = DATA_DIR / "reference"
GEOJSON_FILE = REFERENCE_DIR / "milano-grid.geojson"

# SP6 outputs
PROCESSED_DIR = DATA_DIR / "processed"
ACTIVITY_PARQUET = PROCESSED_DIR / "activity"

ANALYTICS_DIR = DATA_DIR / "analytics"
HOURLY_PARQUET = ANALYTICS_DIR / "hourly_grid_summary"

DASHBOARD_CSV = DATA_DIR / "dashboard_summary.csv"

# Spark warehouse
SPARK_WAREHOUSE = DATA_DIR / "spark_warehouse"

# Log
LOG_FILE = BASE_DIR / "sp6_execution.log"


# ============================================================
# CREATE REQUIRED DIRECTORIES
# ============================================================

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
REFERENCE_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("SP6")


# ============================================================
# WINDOWS HADOOP CONFIGURATION
# ============================================================

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"


# ============================================================
# SPARK SESSION
# ============================================================

spark = None

try:

    spark = (
        SparkSession.builder
        .appName("SP6_Write_Processed_Analytics")
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
    # HELPER FUNCTIONS
    # ========================================================

    def delete_directory(path):
        """Delete an existing directory."""

        if path.exists():
            shutil.rmtree(path)
            logger.info(
                f"Removed existing directory: {path}"
            )


    def directory_size(path):
        """Calculate total size of files inside a directory."""

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
        """Convert bytes to readable units."""

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


    # ========================================================
    # MAIN
    # ========================================================

    print("=" * 70)
    print("SP6 - WRITE PROCESSED & ANALYTICS DATA")
    print("=" * 70)

    logger.info("=" * 70)
    logger.info("SP6 START")
    logger.info("=" * 70)


    # ========================================================
    # 1. CHECK INPUT FILES
    # ========================================================

    print("\n[1] Checking input files...")

    print("SP2 input:")
    print(f"  {CLEAN_ACTIVITY}")

    print("\nSP3 input:")
    print(f"  {HOURLY_SUMMARY}")

    print("\nGeoJSON:")
    print(f"  {GEOJSON_FILE}")

    if not CLEAN_ACTIVITY.exists():

        raise FileNotFoundError(
            f"Clean activity input not found:\n"
            f"{CLEAN_ACTIVITY}"
        )

    if not HOURLY_SUMMARY.exists():

        raise FileNotFoundError(
            f"Hourly summary input not found:\n"
            f"{HOURLY_SUMMARY}"
        )

    if not GEOJSON_FILE.exists():

        raise FileNotFoundError(
            f"GeoJSON reference file not found:\n"
            f"{GEOJSON_FILE}"
        )

    print("\nPASS: All required input paths exist.")

    logger.info(
        "PASS: All required input paths exist."
    )


    # ========================================================
    # 2. READ CLEAN ACTIVITY FROM SP2
    # ========================================================

    print("\n[2] Reading SP2 clean activity...")

    clean_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(CLEAN_ACTIVITY))
    )

    print("\nClean activity schema:")
    clean_df.printSchema()

    clean_count = clean_df.count()

    print(
        f"Clean activity rows: {clean_count:,}"
    )

    logger.info(
        f"Clean activity rows: {clean_count}"
    )


    # ========================================================
    # 3. CHECK REQUIRED CLEAN ACTIVITY COLUMNS
    # ========================================================

    required_clean_columns = [
        "timestamp",
        "grid_id",
        "country_code",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity"
    ]

    missing_clean_columns = [
        c
        for c in required_clean_columns
        if c not in clean_df.columns
    ]

    if missing_clean_columns:

        raise ValueError(
            f"Missing columns in clean activity: "
            f"{missing_clean_columns}"
        )

    print(
        "PASS: Required clean activity columns exist."
    )

    logger.info(
        "PASS: Required clean activity columns exist."
    )


    # ========================================================
    # 4. CAST TIMESTAMP
    # ========================================================

    clean_df = clean_df.withColumn(
        "timestamp",
        col("timestamp").cast("timestamp")
    )

    null_timestamp_count = (
        clean_df
        .filter(col("timestamp").isNull())
        .count()
    )

    if null_timestamp_count > 0:

        raise AssertionError(
            f"Found {null_timestamp_count} NULL timestamps."
        )

    print(
        "PASS: Timestamp validation passed."
    )


    # ========================================================
    # 5. ADD DATE PARTITION COLUMN
    # ========================================================

    print(
        "\n[3] Creating date partition column..."
    )

    clean_df = clean_df.withColumn(
        "date",
        to_date(col("timestamp"))
    )

    null_date_count = (
        clean_df
        .filter(col("date").isNull())
        .count()
    )

    if null_date_count > 0:

        raise AssertionError(
            f"Found {null_date_count} NULL date values."
        )

    print(
        "PASS: Date partition column created."
    )

    logger.info(
        "PASS: Date partition column created."
    )


    # ========================================================
    # 6. WRITE CLEAN ACTIVITY AS PARTITIONED PARQUET
    # ========================================================

    print(
        "\n[4] Writing clean activity as Parquet..."
    )

    print(
        "Using controlled Spark partitioning..."
    )

    logger.info(
        "Starting clean activity Parquet write."
    )

    delete_directory(ACTIVITY_PARQUET)

    (
        clean_df
        .repartition("date")
        .write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(str(ACTIVITY_PARQUET))
    )

    print(
        "PASS: Clean activity written as Parquet."
    )

    print("Output:")
    print(f"  {ACTIVITY_PARQUET}")

    logger.info(
        f"Clean activity Parquet written to "
        f"{ACTIVITY_PARQUET}"
    )


    # ========================================================
    # 7. CHECK DATE PARTITIONS
    # ========================================================

    partitions = get_partition_folders(
        ACTIVITY_PARQUET
    )

    print("\nDate partition folders:")

    for partition in partitions:
        print(f"  {partition}")

    if len(partitions) == 0:

        raise AssertionError(
            "No date partition folders were created."
        )

    print(
        f"PASS: {len(partitions)} date partition(s) found."
    )

    logger.info(
        f"PASS: {len(partitions)} date partitions found."
    )


    # ========================================================
    # 8. ROUND-TRIP VALIDATION FOR CLEAN ACTIVITY
    # ========================================================

    print(
        "\n[5] Round-trip validation of activity Parquet..."
    )

    activity_roundtrip = (
        spark.read
        .parquet(str(ACTIVITY_PARQUET))
    )

    activity_roundtrip_count = (
        activity_roundtrip.count()
    )

    print(
        f"Original rows:   {clean_count:,}"
    )

    print(
        f"Round-trip rows: "
        f"{activity_roundtrip_count:,}"
    )

    if clean_count != activity_roundtrip_count:

        raise AssertionError(
            "Clean activity row count changed "
            "after round trip."
        )

    print(
        "PASS: Activity row count preserved."
    )

    logger.info(
        "PASS: Activity row count preserved."
    )


    # ========================================================
    # 9. READ SP3 HOURLY SUMMARY
    # ========================================================

    print(
        "\n[6] Reading SP3 hourly_grid_summary..."
    )

    hourly_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(HOURLY_SUMMARY))
    )

    print("\nHourly grid summary schema:")
    hourly_df.printSchema()

    hourly_count = hourly_df.count()

    print(
        f"Hourly grid summary rows: "
        f"{hourly_count:,}"
    )

    logger.info(
        f"Hourly grid summary rows: "
        f"{hourly_count}"
    )


    # ========================================================
    # 10. CHECK HOURLY COLUMNS
    # ========================================================

    required_hourly_columns = [
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
    ]

    missing_hourly_columns = [
        c
        for c in required_hourly_columns
        if c not in hourly_df.columns
    ]

    if missing_hourly_columns:

        raise ValueError(
            f"Missing hourly columns: "
            f"{missing_hourly_columns}"
        )

    print(
        "PASS: Required hourly columns exist."
    )


    # ========================================================
    # 11. ENSURE NO GEOMETRY
    # ========================================================

    geometry_columns = [
        c
        for c in hourly_df.columns
        if c.lower() in [
            "geometry",
            "geom",
            "polygon"
        ]
    ]

    if geometry_columns:

        raise AssertionError(
            f"Geometry column found: "
            f"{geometry_columns}"
        )

    print(
        "PASS: No geometry column exists "
        "in hourly_grid_summary."
    )

    logger.info(
        "PASS: No geometry column exists "
        "in hourly_grid_summary."
    )


    # ========================================================
    # 12. DUPLICATE CHECK
    # ========================================================

    print(
        "\n[7] Checking duplicate "
        "(grid_id, timestamp) records..."
    )

    duplicate_groups = (
        hourly_df
        .groupBy("grid_id", "timestamp")
        .count()
        .filter(col("count") > 1)
    )

    duplicate_count = duplicate_groups.count()

    print(
        f"Duplicate groups: {duplicate_count}"
    )

    if duplicate_count > 0:

        raise AssertionError(
            f"Found {duplicate_count} duplicate "
            "(grid_id, timestamp) groups."
        )

    print(
        "PASS: One record per grid and hour."
    )

    logger.info(
        "PASS: No duplicate "
        "(grid_id, timestamp) records."
    )


    # ========================================================
    # 13. WRITE HOURLY SUMMARY AS PARQUET
    # ========================================================

    print(
        "\n[8] Writing hourly_grid_summary as Parquet..."
    )

    logger.info(
        "Starting hourly summary Parquet write."
    )

    delete_directory(HOURLY_PARQUET)

    (
        hourly_df
        .write
        .mode("overwrite")
        .parquet(str(HOURLY_PARQUET))
    )

    print(
        "PASS: hourly_grid_summary written as Parquet."
    )

    print("Output:")
    print(f"  {HOURLY_PARQUET}")

    logger.info(
        f"Hourly Parquet written to "
        f"{HOURLY_PARQUET}"
    )


    # ========================================================
    # 14. ROUND-TRIP VALIDATION OF HOURLY PARQUET
    # ========================================================

    print(
        "\n[9] Round-trip validation "
        "of hourly_grid_summary..."
    )

    hourly_roundtrip = (
        spark.read
        .parquet(str(HOURLY_PARQUET))
    )

    hourly_roundtrip_count = (
        hourly_roundtrip.count()
    )

    print(
        f"Original rows:   {hourly_count:,}"
    )

    print(
        f"Round-trip rows: "
        f"{hourly_roundtrip_count:,}"
    )

    if hourly_count != hourly_roundtrip_count:

        raise AssertionError(
            "Hourly row count changed "
            "after round trip."
        )

    print(
        "PASS: Hourly row count preserved."
    )


    # ========================================================
    # 15. SCHEMA VALIDATION
    # ========================================================

    original_schema = hourly_df.schema
    roundtrip_schema = hourly_roundtrip.schema

    if original_schema != roundtrip_schema:

        print("\nOriginal schema:")
        hourly_df.printSchema()

        print("\nRound-trip schema:")
        hourly_roundtrip.printSchema()

        raise AssertionError(
            "Hourly schema changed "
            "after round trip."
        )

    print(
        "PASS: Hourly schema preserved."
    )

    logger.info(
        "PASS: Hourly schema preserved."
    )


    # ========================================================
    # 16. DUPLICATE CHECK AFTER ROUND TRIP
    # ========================================================

    print(
        "\nChecking duplicates after round trip..."
    )

    roundtrip_duplicate_count = (
        hourly_roundtrip
        .groupBy("grid_id", "timestamp")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if roundtrip_duplicate_count > 0:

        raise AssertionError(
            "Duplicate grid/hour records "
            "found after round trip."
        )

    print(
        "PASS: Duplicate assertion passes "
        "after round trip."
    )

    logger.info(
        "PASS: No duplicates after "
        "hourly round trip."
    )


    # ========================================================
    # 17. GEOMETRY CHECK AFTER ROUND TRIP
    # ========================================================

    roundtrip_geometry_columns = [
        c
        for c in hourly_roundtrip.columns
        if c.lower() in [
            "geometry",
            "geom",
            "polygon"
        ]
    ]

    if roundtrip_geometry_columns:

        raise AssertionError(
            f"Geometry found after round trip: "
            f"{roundtrip_geometry_columns}"
        )

    print(
        "PASS: No geometry exists "
        "after Parquet round trip."
    )


    # ========================================================
    # 18. CREATE DASHBOARD SUMMARY
    # ========================================================

    print(
        "\n[10] Creating dashboard summary CSV..."
    )

    distinct_grids = (
        hourly_df
        .select(countDistinct("grid_id"))
        .collect()[0][0]
    )

    start_timestamp = (
        hourly_df
        .select(spark_min("timestamp"))
        .collect()[0][0]
    )

    end_timestamp = (
        hourly_df
        .select(spark_max("timestamp"))
        .collect()[0][0]
    )

    total_activity = (
        hourly_df
        .select(spark_sum("total_activity"))
        .collect()[0][0]
    )

    total_internet = (
        hourly_df
        .select(spark_sum("internet_activity"))
        .collect()[0][0]
    )

    dashboard_rows = [
        ["metric", "value"],
        ["clean_activity_rows", clean_count],
        ["hourly_grid_rows", hourly_count],
        ["distinct_grids", distinct_grids],
        ["start_timestamp", str(start_timestamp)],
        ["end_timestamp", str(end_timestamp)],
        ["total_activity", total_activity],
        [
            "total_internet_activity",
            total_internet
        ],
        ["date_partitions", len(partitions)]
    ]

    with open(
        DASHBOARD_CSV,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)
        writer.writerows(dashboard_rows)

    print(
        f"PASS: Dashboard summary created:\n"
        f"  {DASHBOARD_CSV}"
    )

    logger.info(
        f"Dashboard summary created: "
        f"{DASHBOARD_CSV}"
    )


    # ========================================================
    # 19. FILE SIZE COMPARISON
    # ========================================================

    print(
        "\n[11] Comparing CSV and Parquet sizes..."
    )

    csv_size = directory_size(
        CLEAN_ACTIVITY
    )

    parquet_activity_size = directory_size(
        ACTIVITY_PARQUET
    )

    parquet_hourly_size = directory_size(
        HOURLY_PARQUET
    )

    print(
        f"\nClean activity CSV size:     "
        f"{format_bytes(csv_size)}"
    )

    print(
        f"Clean activity Parquet size: "
        f"{format_bytes(parquet_activity_size)}"
    )

    print(
        f"Hourly Parquet size:         "
        f"{format_bytes(parquet_hourly_size)}"
    )

    if parquet_activity_size > 0:

        size_ratio = (
            csv_size / parquet_activity_size
        )

        print(
            f"\nCSV / Parquet size ratio: "
            f"{size_ratio:.2f}x"
        )

        logger.info(
            f"CSV size: {csv_size} bytes"
        )

        logger.info(
            f"Activity Parquet size: "
            f"{parquet_activity_size} bytes"
        )

        logger.info(
            f"CSV/Parquet ratio: "
            f"{size_ratio:.2f}x"
        )


    # ========================================================
    # 20. STORAGE DECISION EXPLANATION
    # ========================================================

    print("\n[12] Storage decisions")

    print(
        """
Overwrite vs Append:

- Clean processed activity:
  OVERWRITE is appropriate when rebuilding the complete
  processed dataset from the supplied source files.

- hourly_grid_summary:
  OVERWRITE is appropriate when SP3 is rerun and the
  complete analytics dataset is regenerated.

- Dashboard summary:
  OVERWRITE is appropriate because it represents the latest
  generated dashboard snapshot.

- APPEND would be appropriate when adding genuinely new
  partitions or incremental data without rebuilding existing
  data. It should not be used blindly because repeated runs
  can create duplicate records.

Parquet benefits:

- Columnar storage
- Better compression
- Faster analytical queries
- Reads only required columns
- Preserves data types
- Works efficiently with Spark
- Partitioning allows date-based filtering

Geometry decision:

- Polygon geometry is static reference data.
- Hourly activity is fact-shaped analytics data.
- Duplicating geometry for every grid/hour row would greatly
  increase storage size and mix reference data with facts.
- Therefore geometry remains in milano-grid.geojson.
"""
    )


    # ========================================================
    # 21. FINAL ACCEPTANCE TESTS
    # ========================================================

    print(
        "\n" + "=" * 70
    )

    print(
        "SP6 ACCEPTANCE TESTS"
    )

    print(
        "=" * 70
    )

    tests = {

        "Clean activity Parquet exists":
            ACTIVITY_PARQUET.exists(),

        "Activity is partitioned by date":
            len(partitions) > 0,

        "Hourly summary Parquet exists":
            HOURLY_PARQUET.exists(),

        "Dashboard summary CSV exists":
            DASHBOARD_CSV.exists(),

        "Activity round-trip row count":
            clean_count == activity_roundtrip_count,

        "Hourly round-trip row count":
            hourly_count == hourly_roundtrip_count,

        "Hourly schema preserved":
            original_schema == roundtrip_schema,

        "No duplicate grid/hour records":
            duplicate_count == 0,

        "No duplicates after round trip":
            roundtrip_duplicate_count == 0,

        "No geometry in hourly analytics":
            len(geometry_columns) == 0,

        "No geometry after round trip":
            len(roundtrip_geometry_columns) == 0,

        "GeoJSON retained separately":
            GEOJSON_FILE.exists(),

        "File-size comparison completed":
            (
                csv_size > 0
                and parquet_activity_size > 0
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
    # FINAL RESULT
    # ========================================================

    print(
        "\n" + "=" * 70
    )

    if all_passed:

        print(
            "SP6 COMPLETED SUCCESSFULLY"
        )

        logger.info(
            "SP6 COMPLETED SUCCESSFULLY"
        )

    else:

        print(
            "SP6 FAILED - CHECK THE TESTS ABOVE"
        )

        logger.error(
            "SP6 FAILED"
        )

    print(
        "=" * 70
    )


except Exception as error:

    print(
        "\n" + "=" * 70
    )

    print(
        "SP6 FAILED"
    )

    print(
        "=" * 70
    )

    print(
        f"Error: {error}"
    )

    try:
        logger.exception("SP6 FAILED")
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

        # Ignore secondary JVM/Py4J shutdown errors.
        pass

