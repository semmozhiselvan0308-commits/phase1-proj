from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    hour,
    to_date,
    row_number,
)
from pyspark.sql.window import Window
import sqlite3
import os
import json


# ============================================================
# DE6 - Warehouse Modelling for Network Analytics
# ============================================================

# Project paths
BASE_DIR = r"C:\Users\Admin\phase1-proj\phase3\de6"

SOURCE_PATH = (
    r"C:\Users\Admin\phase1-proj\phase3\de5"
    r"\data\analytics\grid_summary"
)

REFERENCE_PATH = (
    r"C:\Users\Admin\phase1-proj\phase3\de5"
    r"\data\reference\milano-grid.geojson"
)

WAREHOUSE_DIR = os.path.join(
    BASE_DIR,
    "warehouse"
)

DATABASE_PATH = os.path.join(
    WAREHOUSE_DIR,
    "network_analytics.db"
)


# ============================================================
# Spark session
# ============================================================

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("DE6_Warehouse_Model")
    .config(
        "spark.pyspark.python",
        r"C:\Program Files\Python311\python.exe"
    )
    .config(
        "spark.pyspark.driver.python",
        r"C:\Program Files\Python311\python.exe"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Read DE5 analytics output
# ============================================================

print("=" * 70)
print("DE6 - Reading DE5 analytics data")
print("=" * 70)

df = spark.read.parquet(SOURCE_PATH)

# Materialize the source DataFrame so later actions
# do not repeatedly reread the Parquet files.
df = df.cache()

source_row_count = df.count()

print("Source columns:")
print(df.columns)

print("Source row count:", source_row_count)

print("Source schema:")
df.printSchema()


# ============================================================
# Create dim_time
# ============================================================

print("=" * 70)
print("Creating dim_time")
print("=" * 70)

dim_time_base = (
    df
    .select("timestamp")
    .distinct()
    .withColumn(
        "date",
        to_date(col("timestamp"))
    )
    .withColumn(
        "hour",
        hour(col("timestamp"))
    )
    .orderBy("timestamp")
)

# Stable sequential surrogate key
time_window = Window.orderBy("timestamp")

dim_time = (
    dim_time_base
    .withColumn(
        "time_key",
        row_number().over(time_window)
    )
    .select(
        "time_key",
        "timestamp",
        "date",
        "hour"
    )
)

dim_time_count = dim_time.count()

print("dim_time rows:", dim_time_count)

dim_time.show(
    10,
    truncate=False
)


# ============================================================
# Create dim_grid from static GeoJSON reference
# ============================================================

print("=" * 70)
print("Creating dim_grid from Milan GeoJSON reference")
print("=" * 70)

print("GeoJSON reference:")
print(REFERENCE_PATH)

# ------------------------------------------------------------
# Read GeoJSON using Python.
#
# The GeoJSON contains:
#   FeatureCollection
#   10,000 features
#   properties.cellId
#   Polygon geometry
#
# We intentionally do NOT store the full Polygon in SQLite.
# Instead, we store a reference to the source geometry.
# ------------------------------------------------------------

with open(
    REFERENCE_PATH,
    "r",
    encoding="utf-8"
) as geojson_file:

    geojson_data = json.load(geojson_file)


features = geojson_data.get("features", [])

print("GeoJSON type:", geojson_data.get("type"))
print("GeoJSON feature count:", len(features))


if len(features) == 0:
    raise ValueError(
        "GeoJSON contains no features."
    )


grid_records = []

for feature in features:

    properties = feature.get(
        "properties",
        {}
    )

    cell_id = properties.get(
        "cellId"
    )

    if cell_id is None:
        raise ValueError(
            "GeoJSON feature is missing properties.cellId."
        )

    geometry_type = (
        feature.get("geometry", {})
        .get("type")
    )

    geometry_reference = (
        "milano-grid.geojson"
        + "#cellId="
        + str(cell_id)
    )

    grid_records.append(
        (
            int(cell_id),
            geometry_reference
        )
    )


# Check for duplicate GeoJSON cell IDs
grid_ids = [
    record[0]
    for record in grid_records
]

if len(grid_ids) != len(set(grid_ids)):
    raise ValueError(
        "Duplicate cellId values found in GeoJSON."
    )


print(
    "Unique GeoJSON grid IDs:",
    len(set(grid_ids))
)


# ------------------------------------------------------------
# Create Spark DataFrame for dim_grid
# ------------------------------------------------------------

dim_grid_source = spark.createDataFrame(
    grid_records,
    [
        "grid_id",
        "geometry_reference"
    ]
)


# Generate sequential surrogate key
grid_window = Window.orderBy("grid_id")

dim_grid = (
    dim_grid_source
    .withColumn(
        "grid_key",
        row_number().over(grid_window)
    )
    .select(
        "grid_key",
        "grid_id",
        "geometry_reference"
    )
    .orderBy("grid_id")
)

dim_grid_count = dim_grid.count()

print("dim_grid rows:", dim_grid_count)

dim_grid.show(
    10,
    truncate=False
)


# ============================================================
# Validate dim_grid against GeoJSON
# ============================================================

if dim_grid_count != len(features):
    raise ValueError(
        "dim_grid row count does not match "
        "GeoJSON feature count."
    )

print(
    "PASS: dim_grid contains all GeoJSON grid cells."
)


# ============================================================
# Validate observed grids exist in dim_grid
# ============================================================

observed_grid_count = (
    df
    .select("grid_id")
    .distinct()
    .count()
)

print(
    "Distinct grid IDs observed in activity:",
    observed_grid_count
)

missing_observed_grids = (
    df
    .select("grid_id")
    .distinct()
    .join(
        dim_grid.select("grid_id"),
        on="grid_id",
        how="left_anti"
    )
    .count()
)

print(
    "Observed grid IDs missing from dim_grid:",
    missing_observed_grids
)

if missing_observed_grids != 0:
    raise ValueError(
        "Some activity grid IDs are missing from dim_grid."
    )

print(
    "PASS: All observed activity grids exist in dim_grid."
)


# ============================================================
# Create fact_network_activity
# ============================================================

print("=" * 70)
print("Creating fact_network_activity")
print("=" * 70)

fact = (
    df
    .join(
        dim_time.select(
            "time_key",
            "timestamp"
        ),
        on="timestamp",
        how="inner"
    )
    .join(
        dim_grid.select(
            "grid_key",
            "grid_id"
        ),
        on="grid_id",
        how="inner"
    )
    .select(
        "time_key",
        "grid_key",

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

fact_row_count = fact.count()

print(
    "fact_network_activity rows:",
    fact_row_count
)

fact.show(
    10,
    truncate=False
)


# ============================================================
# Fact row-count validation
# ============================================================

if fact_row_count != source_row_count:
    raise ValueError(
        "FACT FAN-OUT/LOSS DETECTED: "
        "fact row count does not match source row count."
    )

print(
    "PASS: Fact row count exactly matches source row count."
)


# ============================================================
# Confirm fact contains no geometry
# ============================================================

fact_columns = fact.columns

if "geometry" in fact_columns:
    raise ValueError(
        "Fact table must not contain geometry."
    )

if "geometry_reference" in fact_columns:
    raise ValueError(
        "Fact table must not contain geometry_reference."
    )

print(
    "PASS: Fact contains no geometry."
)


# ============================================================
# Create warehouse directory
# ============================================================

os.makedirs(
    WAREHOUSE_DIR,
    exist_ok=True
)


# ============================================================
# SQLite database
# ============================================================

print("=" * 70)
print("Creating SQLite warehouse")
print("=" * 70)

connection = sqlite3.connect(
    DATABASE_PATH
)

cursor = connection.cursor()


# ============================================================
# Enable foreign-key enforcement
# ============================================================

cursor.execute(
    "PRAGMA foreign_keys = ON"
)


# ============================================================
# Drop existing tables
# ============================================================

cursor.execute(
    "DROP TABLE IF EXISTS fact_network_activity"
)

cursor.execute(
    "DROP TABLE IF EXISTS dim_time"
)

cursor.execute(
    "DROP TABLE IF EXISTS dim_grid"
)


# ============================================================
# Create dim_time
# ============================================================

cursor.execute(
    """
    CREATE TABLE dim_time (
        time_key INTEGER PRIMARY KEY,
        timestamp TEXT NOT NULL UNIQUE,
        date TEXT NOT NULL,
        hour INTEGER NOT NULL
    )
    """
)


# ============================================================
# Create dim_grid
# ============================================================

cursor.execute(
    """
    CREATE TABLE dim_grid (
        grid_key INTEGER PRIMARY KEY,
        grid_id INTEGER NOT NULL UNIQUE,
        geometry_reference TEXT NOT NULL
    )
    """
)


# ============================================================
# Create fact_network_activity
# ============================================================

cursor.execute(
    """
    CREATE TABLE fact_network_activity (
        time_key INTEGER NOT NULL,
        grid_key INTEGER NOT NULL,

        sms_in REAL,
        sms_out REAL,

        call_in REAL,
        call_out REAL,

        total_sms_activity REAL,
        total_call_activity REAL,

        internet_activity REAL,

        total_activity REAL,

        internet_share REAL,

        FOREIGN KEY (time_key)
            REFERENCES dim_time(time_key),

        FOREIGN KEY (grid_key)
            REFERENCES dim_grid(grid_key)
    )
    """
)


# ============================================================
# Load dim_time
# ============================================================

print("=" * 70)
print("Loading dim_time")
print("=" * 70)

dim_time_rows = []

for row in dim_time.collect():

    dim_time_rows.append(
        (
            int(row["time_key"]),

            row["timestamp"].strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

            row["date"].strftime(
                "%Y-%m-%d"
            ),

            int(row["hour"])
        )
    )


cursor.executemany(
    """
    INSERT INTO dim_time (
        time_key,
        timestamp,
        date,
        hour
    )
    VALUES (?, ?, ?, ?)
    """,
    dim_time_rows
)

print(
    "Inserted dim_time rows:",
    len(dim_time_rows)
)


# ============================================================
# Load dim_grid
# ============================================================

print("=" * 70)
print("Loading dim_grid")
print("=" * 70)

dim_grid_rows = []

for row in dim_grid.collect():

    dim_grid_rows.append(
        (
            int(row["grid_key"]),

            int(row["grid_id"]),

            row["geometry_reference"]
        )
    )


cursor.executemany(
    """
    INSERT INTO dim_grid (
        grid_key,
        grid_id,
        geometry_reference
    )
    VALUES (?, ?, ?)
    """,
    dim_grid_rows
)

print(
    "Inserted dim_grid rows:",
    len(dim_grid_rows)
)


# ============================================================
# Load fact_network_activity
# ============================================================

print("=" * 70)
print("Loading fact_network_activity")
print("=" * 70)

fact_rows = []

for row in fact.collect():

    fact_rows.append(
        (
            int(row["time_key"]),
            int(row["grid_key"]),

            row["sms_in"],
            row["sms_out"],

            row["call_in"],
            row["call_out"],

            row["total_sms_activity"],
            row["total_call_activity"],

            row["internet_activity"],

            row["total_activity"],

            row["internet_share"]
        )
    )


cursor.executemany(
    """
    INSERT INTO fact_network_activity (
        time_key,
        grid_key,

        sms_in,
        sms_out,

        call_in,
        call_out,

        total_sms_activity,
        total_call_activity,

        internet_activity,

        total_activity,

        internet_share
    )
    VALUES (
        ?, ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?,
        ?,
        ?
    )
    """,
    fact_rows
)

print(
    "Inserted fact rows:",
    len(fact_rows)
)


# ============================================================
# Create indexes
# ============================================================

print("=" * 70)
print("Creating indexes")
print("=" * 70)

cursor.execute(
    """
    CREATE INDEX idx_fact_time
    ON fact_network_activity(time_key)
    """
)

cursor.execute(
    """
    CREATE INDEX idx_fact_grid
    ON fact_network_activity(grid_key)
    """
)

cursor.execute(
    """
    CREATE INDEX idx_dim_grid_grid_id
    ON dim_grid(grid_id)
    """
)

cursor.execute(
    """
    CREATE INDEX idx_dim_time_date
    ON dim_time(date)
    """
)

cursor.execute(
    """
    CREATE INDEX idx_fact_internet_activity
    ON fact_network_activity(internet_activity)
    """
)

print(
    "PASS: Required indexes created."
)


# ============================================================
# Commit
# ============================================================

connection.commit()


# ============================================================
# Warehouse validation
# ============================================================

print("=" * 70)
print("Warehouse validation")
print("=" * 70)


# ------------------------------------------------------------
# dim_time count
# ------------------------------------------------------------

cursor.execute(
    """
    SELECT COUNT(*)
    FROM dim_time
    """
)

db_dim_time_count = cursor.fetchone()[0]

print(
    "dim_time rows:",
    db_dim_time_count
)


# ------------------------------------------------------------
# dim_grid count
# ------------------------------------------------------------

cursor.execute(
    """
    SELECT COUNT(*)
    FROM dim_grid
    """
)

db_dim_grid_count = cursor.fetchone()[0]

print(
    "dim_grid rows:",
    db_dim_grid_count
)


# ------------------------------------------------------------
# Fact count
# ------------------------------------------------------------

cursor.execute(
    """
    SELECT COUNT(*)
    FROM fact_network_activity
    """
)

db_fact_count = cursor.fetchone()[0]

print(
    "fact_network_activity rows:",
    db_fact_count
)


# ------------------------------------------------------------
# Duplicate grid validation
# ------------------------------------------------------------

cursor.execute(
    """
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT grid_id) AS distinct_grid_ids
    FROM dim_grid
    """
)

grid_total, grid_distinct = cursor.fetchone()

print(
    "dim_grid total rows:",
    grid_total
)

print(
    "dim_grid distinct grid IDs:",
    grid_distinct
)

if grid_total != grid_distinct:
    raise ValueError(
        "Duplicate grid_id values found in dim_grid."
    )

print(
    "PASS: No duplicate grid_id values."
)


# ------------------------------------------------------------
# Fact total_activity statistics
# ------------------------------------------------------------

cursor.execute(
    """
    SELECT
        MIN(total_activity),
        MAX(total_activity),
        SUM(total_activity)
    FROM fact_network_activity
    """
)

fact_statistics = cursor.fetchone()

print(
    "Fact total_activity statistics:",
    fact_statistics
)


# ============================================================
# Required DE6 analytical queries
# ============================================================

print("=" * 70)
print("DE6 Analytical Query 1 - Top Grids")
print("=" * 70)

cursor.execute(
    """
    SELECT
        dg.grid_id,
        SUM(f.total_activity) AS total_activity
    FROM fact_network_activity f
    JOIN dim_grid dg
        ON f.grid_key = dg.grid_key
    GROUP BY dg.grid_id
    ORDER BY total_activity DESC
    LIMIT 10
    """
)

top_grids = cursor.fetchall()

for row in top_grids:
    print(row)


# ============================================================

print("=" * 70)
print("DE6 Analytical Query 2 - Hourly Trends")
print("=" * 70)

cursor.execute(
    """
    SELECT
        dt.hour,
        SUM(f.total_activity) AS total_activity
    FROM fact_network_activity f
    JOIN dim_time dt
        ON f.time_key = dt.time_key
    GROUP BY dt.hour
    ORDER BY dt.hour
    """
)

hourly_trends = cursor.fetchall()

for row in hourly_trends:
    print(row)


# ============================================================

print("=" * 70)
print("DE6 Analytical Query 3 - Internet Heavy Windows")
print("=" * 70)

cursor.execute(
    """
    SELECT
        dt.timestamp,
        dg.grid_id,
        f.internet_activity,
        f.total_activity,
        f.internet_share
    FROM fact_network_activity f
    JOIN dim_time dt
        ON f.time_key = dt.time_key
    JOIN dim_grid dg
        ON f.grid_key = dg.grid_key
    ORDER BY f.internet_share DESC
    LIMIT 10
    """
)

internet_heavy = cursor.fetchall()

for row in internet_heavy:
    print(row)


# ============================================================
# Final acceptance checks
# ============================================================

print("=" * 70)
print("DE6 Acceptance Checks")
print("=" * 70)


# Check 1 - fact count
if db_fact_count == source_row_count:
    print(
        "PASS: Fact row count equals source row count."
    )
else:
    print(
        "FAIL: Fact row count does not equal source row count."
    )


# Check 2 - dim_grid count
if db_dim_grid_count == len(features):
    print(
        "PASS: dim_grid contains exactly 10,000 GeoJSON grids."
    )
else:
    print(
        "FAIL: dim_grid count does not equal GeoJSON feature count."
    )


# Check 3 - duplicate grids
if grid_total == grid_distinct:
    print(
        "PASS: dim_grid has no duplicate grid_id values."
    )
else:
    print(
        "FAIL: dim_grid contains duplicate grid_id values."
    )


# Check 4 - geometry separation
cursor.execute(
    """
    PRAGMA table_info(fact_network_activity)
    """
)

fact_schema = cursor.fetchall()

fact_column_names = [
    column[1]
    for column in fact_schema
]

if (
    "geometry" not in fact_column_names
    and "geometry_reference" not in fact_column_names
):
    print(
        "PASS: Fact table contains no geometry."
    )
else:
    print(
        "FAIL: Geometry found in fact table."
    )


# Check 5 - database path
print(
    "PASS: SQLite warehouse created at:"
)

print(
    DATABASE_PATH
)


# ============================================================
# Close resources
# ============================================================

connection.close()

spark.stop()


# ============================================================
# Completion
# ============================================================

print("=" * 70)
print("DE6 warehouse build completed successfully")
print("=" * 70)

print(
    "Database:",
    DATABASE_PATH
)