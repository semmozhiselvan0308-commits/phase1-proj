from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    to_date,
    broadcast,
    explode
)
from pyspark import StorageLevel
import time


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("SP5_Performance")
    .master("local[*]")
    # Disable automatic broadcast so the normal join
    # remains a genuine non-broadcast join.
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. LOAD SP2 CLEAN DATA
# ============================================================

input_path = "../sp2/sp2_output/clean_network.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("\n========== DATASET INFORMATION ==========")

row_count = df.count()
print("Rows:", row_count)

print("Columns:")
df.printSchema()

print("Partitions:", df.rdd.getNumPartitions())


# ============================================================
# 3. CREATE DATE COLUMN
# ============================================================

df = df.withColumn(
    "date",
    to_date(col("timestamp"))
)


# ============================================================
# 4. BASELINE AGGREGATION
# ============================================================

aggregation = (
    df.groupBy("date", "grid_id")
      .agg(
          sum("sms_in").alias("total_sms_in"),
          sum("sms_out").alias("total_sms_out"),
          sum("call_in").alias("total_call_in"),
          sum("call_out").alias("total_call_out"),
          sum("internet_activity").alias("total_internet")
      )
)


# ============================================================
# 5. EXPLAIN PHYSICAL PLAN
# ============================================================

print("\n========== PHYSICAL PLAN ==========")

aggregation.explain(True)


# ============================================================
# 6. CACHE EXPERIMENT
# ============================================================

print("\n========== CACHE EXPERIMENT ==========")


# ----------------------------
# Without cache - Action 1
# ----------------------------

start = time.perf_counter()

aggregation.count()

end = time.perf_counter()

print(
    f"Without cache - Action 1: {end - start:.3f} seconds"
)


# ----------------------------
# Without cache - Action 2
# ----------------------------

start = time.perf_counter()

aggregation.count()

end = time.perf_counter()

print(
    f"Without cache - Action 2: {end - start:.3f} seconds"
)


# ----------------------------
# Cache aggregation
# ----------------------------

cached_aggregation = aggregation.persist(
    StorageLevel.MEMORY_AND_DISK
)


# ----------------------------
# With cache - Action 1
# ----------------------------

start = time.perf_counter()

cached_aggregation.count()

end = time.perf_counter()

print(
    f"With cache - Action 1: {end - start:.3f} seconds"
)


# ----------------------------
# With cache - Action 2
# ----------------------------

start = time.perf_counter()

cached_aggregation.count()

end = time.perf_counter()

print(
    f"With cache - Action 2: {end - start:.3f} seconds"
)


# Remove cache
cached_aggregation.unpersist()


# ============================================================
# 7. REPARTITION EXPERIMENT
# ============================================================

print("\n========== REPARTITION EXPERIMENT ==========")

original_partitions = df.rdd.getNumPartitions()

print(
    "Original partitions:",
    original_partitions
)


repartitioned_df = df.repartition("date")

repartitioned_partitions = repartitioned_df.rdd.getNumPartitions()

print(
    "Partitions after repartition by date:",
    repartitioned_partitions
)

print(
    "Shuffle partitions setting:",
    spark.conf.get("spark.sql.shuffle.partitions")
)


# ============================================================
# 8. COLUMN PRUNING EXPERIMENT
# ============================================================

print("\n========== COLUMN PRUNING EXPERIMENT ==========")

pruned_df = df.select(
    "timestamp",
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity"
)

print("\nPruned DataFrame schema:")

pruned_df.printSchema()


pruned_aggregation = (
    pruned_df
    .withColumn(
        "date",
        to_date(col("timestamp"))
    )
    .groupBy("date", "grid_id")
    .agg(
        sum("sms_in").alias("total_sms_in"),
        sum("sms_out").alias("total_sms_out"),
        sum("call_in").alias("total_call_in"),
        sum("call_out").alias("total_call_out"),
        sum("internet_activity").alias("total_internet")
    )
)


print("\nPruned aggregation physical plan:")

pruned_aggregation.explain(True)


# ============================================================
# 9. LOAD AND FLATTEN MILANO GRID GEOJSON
# ============================================================

print("\n========== BROADCAST JOIN EXPERIMENT ==========")

grid_path = "../data/reference/milano-grid.geojson"


# Read raw GeoJSON
raw_grid_df = (
    spark.read
    .option("multiline", True)
    .json(grid_path)
)


print("\nRaw GeoJSON schema:")

raw_grid_df.printSchema()


# ============================================================
# 10. FLATTEN GEOJSON FEATURES
# ============================================================

grid_df = (
    raw_grid_df
    .select(
        explode(col("features")).alias("feature")
    )
    .select(
        col("feature.properties.cellId")
            .cast("int")
            .alias("grid_id"),
        col("feature.geometry")
            .alias("geometry")
    )
    .filter(
        col("grid_id").isNotNull()
    )
)


print("\nFlattened grid lookup schema:")

grid_df.printSchema()


grid_count = grid_df.count()

print("\nGrid lookup count:")

print(grid_count)


print("\nSample grid lookup:")

grid_df.show(5, truncate=False)


# ============================================================
# 11. VALIDATE GRID LOOKUP
# ============================================================

print("\n========== GRID LOOKUP VALIDATION ==========")

duplicate_grid_ids = (
    grid_df
    .groupBy("grid_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(
    "Duplicate grid IDs:",
    duplicate_grid_ids
)


null_grid_ids = (
    grid_df
    .filter(col("grid_id").isNull())
    .count()
)

print(
    "Null grid IDs:",
    null_grid_ids
)


# ============================================================
# 12. NORMAL NON-BROADCAST JOIN
# ============================================================

print("\n---------- NORMAL JOIN PLAN ----------")

print(
    "Automatic broadcast disabled:",
    spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
)


normal_join = (
    aggregation.alias("a")
    .join(
        grid_df.alias("g"),
        col("a.grid_id") == col("g.grid_id"),
        "left"
    )
)


normal_join.explain(True)


# ============================================================
# 13. BROADCAST JOIN
# ============================================================

print("\n---------- BROADCAST JOIN PLAN ----------")

broadcast_join = (
    aggregation.alias("a")
    .join(
        broadcast(grid_df).alias("g"),
        col("a.grid_id") == col("g.grid_id"),
        "left"
    )
)


broadcast_join.explain(True)


# ============================================================
# 14. JOIN PERFORMANCE COMPARISON
# ============================================================

print("\n========== JOIN PERFORMANCE COMPARISON ==========")


# ----------------------------
# Normal join timing
# ----------------------------

start = time.perf_counter()

normal_join_count = normal_join.count()

end = time.perf_counter()

normal_join_time = end - start

print(
    f"Normal join rows: {normal_join_count}"
)

print(
    f"Normal join time: {normal_join_time:.3f} seconds"
)


# ----------------------------
# Broadcast join timing
# ----------------------------

start = time.perf_counter()

broadcast_join_count = broadcast_join.count()

end = time.perf_counter()

broadcast_join_time = end - start

print(
    f"Broadcast join rows: {broadcast_join_count}"
)

print(
    f"Broadcast join time: {broadcast_join_time:.3f} seconds"
)


# ============================================================
# 15. PERFORMANCE SUMMARY
# ============================================================

print("\n========== SP5 PERFORMANCE SUMMARY ==========")

print(
    "Input rows:",
    row_count
)

print(
    "Input partitions:",
    original_partitions
)

print(
    "Grid lookup rows:",
    grid_count
)

print(
    "Normal join rows:",
    normal_join_count
)

print(
    "Broadcast join rows:",
    broadcast_join_count
)

print(
    f"Normal join time: {normal_join_time:.3f} seconds"
)

print(
    f"Broadcast join time: {broadcast_join_time:.3f} seconds"
)


if normal_join_time > broadcast_join_time:

    improvement = (
        (normal_join_time - broadcast_join_time)
        / normal_join_time
    ) * 100

    print(
        f"Broadcast join improvement: {improvement:.2f}%"
    )

else:

    print(
        "Broadcast join was not faster in this local run."
    )


# ============================================================
# 16. STOP SPARK
# ============================================================

try:
    # all SP5 processing
    pass

finally:
    spark.stop()
    print("\n========== SP5 COMPLETED SUCCESSFULLY ==========")
