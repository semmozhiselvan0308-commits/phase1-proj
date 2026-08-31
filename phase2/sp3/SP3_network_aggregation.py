
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg,
    to_date,
    hour,
    desc,
    row_number,
    when
)
from pyspark.sql.window import Window


# ============================================================
# SP3 NETWORK ACTIVITY AGGREGATIONS
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SP2_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "sp2"))

INPUT_FILE = os.path.join(SP2_DIR, "sp2_output", "clean_network.csv")

OUTPUT_HOURLY = os.path.join(BASE_DIR, "hourly_grid_summary.csv")
OUTPUT_DAILY = os.path.join(BASE_DIR, "daily_traffic_summary.csv")
OUTPUT_HOTSPOTS = os.path.join(BASE_DIR, "hotspot_ranking.csv")
OUTPUT_PEAK = os.path.join(BASE_DIR, "peak_activity_hour.csv")

LOG_FILE = os.path.join(BASE_DIR, "sp3_execution.log")


# ============================================================ or flow phase
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("SP3 Network Activity Aggregations")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# HELPER: WRITE SINGLE CSV DIRECTORY AS CSV FILE
# ============================================================

def write_csv(df, output_file):
    """
    Write a Spark DataFrame as a single CSV file.
    Spark normally creates a directory, so this helper
    temporarily writes to a directory and renames the
    generated part file.
    """

    temp_dir = output_file + "_tmp"

    if os.path.exists(temp_dir):
        import shutil
        shutil.rmtree(temp_dir)

    if os.path.exists(output_file):
        os.remove(output_file)

    df.coalesce(1).write.mode("overwrite").option(
        "header", True
    ).csv(temp_dir)

    part_file = None

    for filename in os.listdir(temp_dir):
        if filename.startswith("part-") and filename.endswith(".csv"):
            part_file = os.path.join(temp_dir, filename)
            break

    if part_file is None:
        raise RuntimeError(f"No CSV part file generated for {output_file}")

    os.replace(part_file, output_file)

    import shutil
    shutil.rmtree(temp_dir)


# ============================================================
# MAIN
# ============================================================

try:

    logger.info("=" * 70)
    logger.info("SP3 NETWORK ACTIVITY AGGREGATIONS")
    logger.info("=" * 70)

    # ========================================================
    # 1. LOAD SP2 CLEAN DATA
    # ========================================================

    logger.info("Loading clean_network.csv")

    clean_network_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_FILE)
    )

    raw_count = clean_network_df.count()

    logger.info(f"Clean SP2 records: {raw_count:,}")

    # ========================================================
    # CHECK REQUIRED COLUMNS
    # ========================================================

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
        c for c in required_columns
        if c not in clean_network_df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    logger.info("[PASS] Required columns present")

    # ========================================================
    # 2. COUNTRY-CODE → GRID/HOUR GRAIN
    # ========================================================

    logger.info("")
    logger.info("Consolidating country-code records...")
    logger.info(
        "Target grain: exactly one row per timestamp + grid_id"
    )

    activity_columns = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity"
    ]

    hourly_grid_summary = (
        clean_network_df
        .groupBy("timestamp", "grid_id")
        .agg(
            spark_sum("sms_in").alias("sms_in"),
            spark_sum("sms_out").alias("sms_out"),
            spark_sum("call_in").alias("call_in"),
            spark_sum("call_out").alias("call_out"),
            spark_sum("internet_activity").alias("internet_activity")
        )
    )

    # ========================================================
    # 3. COMPUTE OPERATIONAL KPIs
    # ========================================================

    hourly_grid_summary = (
        hourly_grid_summary
        .withColumn(
            "total_sms_activity",
            col("sms_in") + col("sms_out")
        )
        .withColumn(
            "total_call_activity",
            col("call_in") + col("call_out")
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
                col("internet_activity") / col("total_activity")
            ).otherwise(0.0)
        )
        .select(
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
    )

    # ========================================================
    # 4. CACHE CANONICAL ANALYTICS DATAFRAME
    # ========================================================

    hourly_grid_summary.cache()

    hourly_count = hourly_grid_summary.count()

    logger.info(
        f"hourly_grid_summary rows: {hourly_count:,}"
    )

    # ========================================================
    # 5. ACCEPTANCE TEST:
    # ZERO DUPLICATES ON GRID + TIMESTAMP
    # ========================================================

    duplicate_count = (
        hourly_grid_summary
        .groupBy("grid_id", "timestamp")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    assert duplicate_count == 0, (
        f"FAIL: Found {duplicate_count} duplicate "
        f"(grid_id, timestamp) records"
    )

    logger.info(
        "[PASS] Zero duplicates on (grid_id, timestamp)"
    )

    # ========================================================
    # 6. ACCEPTANCE TEST:
    # OUTPUT MUST BE SMALLER THAN INPUT
    # ========================================================

    assert hourly_count < raw_count, (
        "FAIL: hourly_grid_summary is not smaller "
        "than clean_network_df"
    )

    logger.info(
        "[PASS] hourly_grid_summary row count is "
        "strictly less than clean_network_df"
    )

    # ========================================================
    # 7. ACCEPTANCE TEST:
    # D × 24 × 10000
    # ========================================================

    distinct_days = (
        clean_network_df
        .select(to_date("timestamp").alias("day"))
        .distinct()
        .count()
    )

    maximum_allowed_rows = distinct_days * 24 * 10000

    assert hourly_count <= maximum_allowed_rows, (
        f"FAIL: {hourly_count:,} rows exceeds "
        f"D × 24 × 10000 = {maximum_allowed_rows:,}"
    )

    logger.info(
        "[PASS] Row count satisfies D × 24 × 10000 limit"
    )

    # ========================================================
    # 8. ACCEPTANCE TEST:
    # country_code MUST NOT EXIST
    # ========================================================

    assert "country_code" not in hourly_grid_summary.columns, (
        "FAIL: country_code exists in hourly_grid_summary"
    )

    logger.info(
        "[PASS] country_code absent from hourly_grid_summary"
    )

    # ========================================================
    # 9. HAND-CHECK VALIDATION
    # ========================================================

    logger.info("")
    logger.info("Running hand-check validation...")

    sample_key = (
        clean_network_df
        .select("timestamp", "grid_id")
        .groupBy("timestamp", "grid_id")
        .count()
        .orderBy(desc("count"))
        .first()
    )

    if sample_key is None:
        raise ValueError("No records available for hand-check")

    sample_timestamp = sample_key["timestamp"]
    sample_grid = sample_key["grid_id"]

    original_sample = (
        clean_network_df
        .filter(
            (col("timestamp") == sample_timestamp)
            & (col("grid_id") == sample_grid)
        )
        .agg(
            spark_sum("sms_in").alias("sms_in"),
            spark_sum("sms_out").alias("sms_out"),
            spark_sum("call_in").alias("call_in"),
            spark_sum("call_out").alias("call_out"),
            spark_sum("internet_activity").alias(
                "internet_activity"
            )
        )
        .first()
    )

    aggregated_sample = (
        hourly_grid_summary
        .filter(
            (col("timestamp") == sample_timestamp)
            & (col("grid_id") == sample_grid)
        )
        .first()
    )

    for column_name in activity_columns:

        original_value = original_sample[column_name]
        aggregated_value = aggregated_sample[column_name]

        if original_value != aggregated_value:
            raise AssertionError(
                f"Hand-check failed for {column_name}: "
                f"original={original_value}, "
                f"aggregated={aggregated_value}"
            )

    logger.info(
        "[PASS] Hand-check reproduced aggregation exactly"
    )

    logger.info(
        f"Hand-check grid_id={sample_grid}, "
        f"timestamp={sample_timestamp}"
    )

    # ========================================================
    # 10. DAILY TRAFFIC SUMMARY
    # ========================================================

    logger.info("")
    logger.info("Creating daily_traffic_summary...")

    daily_traffic_summary = (
        hourly_grid_summary
        .withColumn("date", to_date("timestamp"))
        .groupBy("date", "grid_id")
        .agg(
            spark_sum("total_sms_activity").alias(
                "total_sms_activity"
            ),
            spark_sum("total_call_activity").alias(
                "total_call_activity"
            ),
            spark_sum("internet_activity").alias(
                "internet_activity"
            ),
            spark_sum("total_activity").alias(
                "total_activity"
            ),
            avg("internet_share").alias(
                "avg_internet_share"
            )
        )
        .orderBy("date", "grid_id")
    )

    daily_count = daily_traffic_summary.count()

    logger.info(
        f"daily_traffic_summary rows: {daily_count:,}"
    )

    # ========================================================
    # 11. TOP 10 HOTSPOT GRIDS
    # ========================================================

    logger.info("")
    logger.info("Creating hotspot ranking...")

    hotspot_ranking = (
        hourly_grid_summary
        .groupBy("grid_id")
        .agg(
            spark_sum("total_activity").alias(
                "total_activity"
            ),
            avg("total_activity").alias(
                "avg_activity"
            )
        )
        .orderBy(desc("total_activity"))
        .limit(10)
    )

    logger.info("Top 10 high-activity grids:")

    for row in hotspot_ranking.collect():
        logger.info(
            f"grid_id={row['grid_id']}, "
            f"total_activity={row['total_activity']}, "
            f"avg_activity={row['avg_activity']}"
        )

    # ========================================================
    # 12. PEAK ACTIVITY HOUR
    # ========================================================

    logger.info("")
    logger.info("Finding peak activity hour...")

    peak_activity_hour = (
        hourly_grid_summary
        .groupBy("timestamp")
        .agg(
            spark_sum("total_activity").alias(
                "total_activity"
            )
        )
        .orderBy(desc("total_activity"))
        .limit(1)
    )

    peak_row = peak_activity_hour.first()

    if peak_row:
        logger.info(
            f"[PASS] Peak activity hour: "
            f"{peak_row['timestamp']} "
            f"with activity={peak_row['total_activity']}"
        )

    # ========================================================
    # 13. WRITE OUTPUTS
    # ========================================================

    logger.info("")
    logger.info("Writing SP3 outputs...")

    write_csv(
        hourly_grid_summary.orderBy(
            "timestamp", "grid_id"
        ),
        OUTPUT_HOURLY
    )

    write_csv(
        daily_traffic_summary,
        OUTPUT_DAILY
    )

    write_csv(
        hotspot_ranking,
        OUTPUT_HOTSPOTS
    )

    write_csv(
        peak_activity_hour,
        OUTPUT_PEAK
    )

    logger.info("[PASS] hourly_grid_summary created")
    logger.info("[PASS] daily_traffic_summary created")
    logger.info("[PASS] hotspot_ranking created")
    logger.info("[PASS] peak_activity_hour created")

    # ========================================================
    # 14. FINAL ACCEPTANCE TESTS
    # ========================================================

    logger.info("")
    logger.info("=" * 70)
    logger.info("SP3 ACCEPTANCE TESTS")
    logger.info("=" * 70)

    logger.info("[PASS] Zero duplicates on (grid_id, timestamp)")
    logger.info("[PASS] hourly row count < clean row count")
    logger.info("[PASS] Row count <= D × 24 × 10000")
    logger.info("[PASS] Hand-check reproduced aggregation")
    logger.info("[PASS] country_code absent from hourly_grid_summary")

    logger.info("")
    logger.info("SP3 RESULT: ALL ACCEPTANCE TESTS PASSED")
    logger.info("")
    logger.info("SP3 EXECUTION COMPLETED")

except Exception as e:

    logger.error("=" * 70)
    logger.error("SP3 FAILED")
    logger.error(str(e))
    logger.error("=" * 70)

    raise

finally:

    spark.stop()
