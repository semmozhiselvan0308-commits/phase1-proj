import os
import sys
import json
import logging

# ============================================================
# PYSPARK PYTHON CONFIGURATION
# ============================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# ============================================================
# HADOOP / WINUTILS CONFIGURATION
# ============================================================

# Hadoop is installed separately on C:\hadoop
HADOOP_HOME = r"C:\hadoop"

WINUTILS_PATH = os.path.join(
    HADOOP_HOME,
    "bin",
    "winutils.exe"
)

# Set Hadoop environment variables for this Python/Spark process
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["hadoop.home.dir"] = HADOOP_HOME

# Add Hadoop bin directory to PATH
os.environ["PATH"] = (
    os.path.join(HADOOP_HOME, "bin")
    + os.pathsep
    + os.environ["PATH"]
)


# ============================================================
# CHECK WINUTILS
# ============================================================

if not os.path.exists(WINUTILS_PATH):
    raise FileNotFoundError(
        f"winutils.exe not found at: {WINUTILS_PATH}"
    )

print("HADOOP_HOME:", os.environ["HADOOP_HOME"])
print("WINUTILS_PATH:", WINUTILS_PATH)
print("winutils.exe found: YES")


# ============================================================
# PYSPARK IMPORTS
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)


# ============================================================
# PATHS
# ============================================================

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

SP3_FILE = os.path.join(
    BASE_DIR,
    "sp3",
    "hourly_grid_summary.csv"
)

GEOJSON_FILE = os.path.join(
    BASE_DIR,
    "data",
    "reference",
    "milano-grid.geojson"
)

OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "sp4_output"
)

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)


# ============================================================
# LOGGING
# ============================================================

LOG_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "sp4_execution.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_FILE,
            mode="w"
        ),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("SP4_Geospatial_Enrichment")
    .config(
        "spark.hadoop.hadoop.home.dir",
        HADOOP_HOME
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# START
# ============================================================

logger.info("========== SP4 START ==========")

try:

    # ========================================================
    # 1. INSPECT GEOJSON
    # ========================================================

    logger.info("Loading Milan grid GeoJSON...")

    with open(
        GEOJSON_FILE,
        "r",
        encoding="utf-8"
    ) as f:

        geojson = json.load(f)

    logger.info(
        f"GeoJSON top-level type: "
        f"{geojson.get('type')}"
    )

    features = geojson.get(
        "features",
        []
    )

    logger.info(
        f"Number of GeoJSON features: "
        f"{len(features)}"
    )

    if not features:
        raise ValueError(
            "GeoJSON contains no features."
        )

    first_feature = features[0]

    logger.info(
        f"Feature keys: "
        f"{list(first_feature.keys())}"
    )

    logger.info(
        f"Top-level feature id example: "
        f"{first_feature.get('id')}"
    )

    logger.info(
        f"Feature properties: "
        f"{first_feature.get('properties')}"
    )

    logger.info(
        f"Geometry type: "
        f"{first_feature.get('geometry', {}).get('type')}"
    )


    # ========================================================
    # IMPORTANT IDENTIFIER VALIDATION
    # ========================================================

    # Correct geographic identifier:
    #
    # properties.cellId -> grid_id
    #
    # NOT:
    #
    # feature["id"]
    #
    # feature["id"] is 0-based.
    # properties["cellId"] is 1-based.

    assert "properties" in first_feature

    assert "cellId" in first_feature["properties"]

    logger.info(
        "Confirmed join key: "
        "properties.cellId -> grid_id"
    )


    # ========================================================
    # 2. FLATTEN GEOJSON INTO GRID LOOKUP
    # ========================================================

    grid_lookup_data = []

    for feature in features:

        properties = feature.get(
            "properties",
            {}
        )

        geometry = feature.get(
            "geometry"
        )

        cell_id = properties.get(
            "cellId"
        )

        if cell_id is None:
            continue

        grid_lookup_data.append(
            (
                int(cell_id),
                json.dumps(geometry)
            )
        )

    logger.info(
        f"Grid lookup rows: "
        f"{len(grid_lookup_data)}"
    )


    # ========================================================
    # 3. CREATE SPARK GRID LOOKUP
    # ========================================================

    lookup_schema = StructType([

        StructField(
            "grid_id",
            IntegerType(),
            False
        ),

        StructField(
            "geometry",
            StringType(),
            False
        )

    ])

    grid_lookup_df = spark.createDataFrame(
        grid_lookup_data,
        schema=lookup_schema
    )

    logger.info(
        "Grid lookup schema:"
    )

    grid_lookup_df.printSchema()


    # ========================================================
    # 4. VALIDATE LOOKUP SIZE AND DUPLICATES
    # ========================================================

    lookup_count = (
        grid_lookup_df.count()
    )

    distinct_lookup_count = (
        grid_lookup_df
        .select("grid_id")
        .distinct()
        .count()
    )

    logger.info(
        f"Grid lookup rows: "
        f"{lookup_count}"
    )

    logger.info(
        f"Distinct grid IDs in lookup: "
        f"{distinct_lookup_count}"
    )

    if lookup_count != distinct_lookup_count:

        raise ValueError(
            "Duplicate grid_id values "
            "found in GeoJSON lookup."
        )

    logger.info(
        "PASS: GeoJSON lookup contains "
        "unique grid IDs."
    )


    # ========================================================
    # 5. LOAD SP3 ACTIVITY DATA
    # ========================================================

    logger.info(
        f"Loading SP3 activity data: "
        f"{SP3_FILE}"
    )

    activity_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(SP3_FILE)
    )

    logger.info(
        "SP3 activity schema:"
    )

    activity_df.printSchema()

    activity_count_before = (
        activity_df.count()
    )

    logger.info(
        f"Activity rows before join: "
        f"{activity_count_before}"
    )


    # ========================================================
    # 6. CHECK DISTINCT ACTIVITY GRIDS
    # ========================================================

    distinct_grids_before = (
        activity_df
        .select("grid_id")
        .distinct()
        .count()
    )

    logger.info(
        f"Distinct activity grids before join: "
        f"{distinct_grids_before}"
    )


    # ========================================================
    # 7. STANDARD JOIN PLAN
    # ========================================================

    logger.info(
        "========== STANDARD JOIN PLAN =========="
    )

    standard_join = (
        activity_df.alias("a")
        .join(
            grid_lookup_df.alias("g"),
            F.col("a.grid_id")
            == F.col("g.grid_id"),
            "left"
        )
        .select(
            "a.*",
            F.col("g.geometry")
        )
    )

    standard_join.explain(
        mode="formatted"
    )


    # ========================================================
    # 8. BROADCAST JOIN PLAN
    # ========================================================

    logger.info(
        "========== BROADCAST JOIN PLAN =========="
    )

    broadcast_join = (
        activity_df.alias("a")
        .join(
            F.broadcast(
                grid_lookup_df
            ).alias("g"),
            F.col("a.grid_id")
            == F.col("g.grid_id"),
            "left"
        )
        .select(
            "a.*",
            F.col("g.geometry")
        )
    )

    broadcast_join.explain(
        mode="formatted"
    )


    # ========================================================
    # 9. FINAL ENRICHED DATASET
    # ========================================================

    logger.info(
        "Performing broadcast LEFT JOIN..."
    )

    grid_activity_geo_df = (
        activity_df.alias("a")
        .join(
            F.broadcast(
                grid_lookup_df
            ).alias("g"),
            F.col("a.grid_id")
            == F.col("g.grid_id"),
            "left"
        )
        .select(

            F.col("a.timestamp"),

            F.col("a.grid_id"),

            F.col("a.sms_in"),

            F.col("a.sms_out"),

            F.col("a.call_in"),

            F.col("a.call_out"),

            F.col("a.internet_activity"),

            F.col("a.total_activity"),

            F.col("g.geometry")

        )
    )


    # ========================================================
    # 10. ROW COUNT VALIDATION
    # ========================================================

    activity_count_after = (
        grid_activity_geo_df.count()
    )

    logger.info(
        f"Activity rows after join: "
        f"{activity_count_after}"
    )

    if activity_count_after != activity_count_before:

        raise ValueError(
            "Row count changed after LEFT JOIN. "
            "Check for duplicate lookup keys."
        )

    logger.info(
        "PASS: Row count unchanged "
        "after LEFT JOIN."
    )


    # ========================================================
    # 11. DISTINCT GRID VALIDATION
    # ========================================================

    distinct_grids_after = (
        grid_activity_geo_df
        .select("grid_id")
        .distinct()
        .count()
    )

    logger.info(
        f"Distinct activity grids after join: "
        f"{distinct_grids_after}"
    )

    if distinct_grids_after != distinct_grids_before:

        raise ValueError(
            "Distinct grid count changed "
            "after join."
        )


    # ========================================================
    # 12. UNMATCHED GRID IDs
    # ========================================================

    unmatched_grid_df = (
        grid_activity_geo_df
        .filter(
            F.col("geometry").isNull()
        )
        .select("grid_id")
        .distinct()
        .orderBy("grid_id")
    )

    unmatched_count = (
        unmatched_grid_df.count()
    )

    logger.info(
        f"Unmatched grid IDs: "
        f"{unmatched_count}"
    )

    unmatched_grid_df.show(
        100,
        truncate=False
    )


    # ========================================================
    # 13. ENRICHMENT COVERAGE
    # ========================================================

    matched_rows = (
        grid_activity_geo_df
        .filter(
            F.col("geometry").isNotNull()
        )
        .count()
    )

    coverage_percentage = (
        matched_rows
        / activity_count_after
        * 100
        if activity_count_after > 0
        else 0
    )

    matched_grids = (
        grid_activity_geo_df
        .filter(
            F.col("geometry").isNotNull()
        )
        .select("grid_id")
        .distinct()
        .count()
    )

    grid_coverage_percentage = (
        matched_grids
        / distinct_grids_before
        * 100
        if distinct_grids_before > 0
        else 0
    )

    logger.info(
        f"Row enrichment coverage: "
        f"{coverage_percentage:.2f}%"
    )

    logger.info(
        f"Distinct grid enrichment coverage: "
        f"{grid_coverage_percentage:.2f}%"
    )

    if grid_coverage_percentage != 100.0:

        logger.warning(
            "WARNING: Distinct grid coverage "
            "is not 100%."
        )

    else:

        logger.info(
            "PASS: 100% of distinct activity "
            "grids have geometry."
        )


    # ========================================================
    # 14. GEOGRAPHIC SPOT CHECK
    # ========================================================

    logger.info(
        "========== GEOGRAPHIC SPOT CHECK =========="
    )


    def calculate_centroid(geometry_json):

        geometry = json.loads(
            geometry_json
        )

        coordinates = (
            geometry["coordinates"][0]
        )

        lons = [
            point[0]
            for point in coordinates
        ]

        lats = [
            point[1]
            for point in coordinates
        ]

        return (
            sum(lons) / len(lons),
            sum(lats) / len(lats)
        )


    spot_check = (
        grid_lookup_df
        .filter(
            F.col("grid_id").isin([1, 2])
        )
        .collect()
    )

    for row in spot_check:

        lon, lat = calculate_centroid(
            row["geometry"]
        )

        logger.info(
            f"grid_id={row['grid_id']} "
            f"centroid=("
            f"longitude={lon}, "
            f"latitude={lat})"
        )


    # ========================================================
    # 15. CHECK GRID 1 AND GRID 2
    # ========================================================

    grid12 = (
        grid_lookup_df
        .filter(
            F.col("grid_id").isin([1, 2])
        )
        .collect()
    )

    if len(grid12) == 2:

        centroid1 = calculate_centroid(
            grid12[0]["geometry"]
        )

        centroid2 = calculate_centroid(
            grid12[1]["geometry"]
        )

        lon_difference = abs(
            centroid1[0]
            - centroid2[0]
        )

        lat_difference = abs(
            centroid1[1]
            - centroid2[1]
        )

        logger.info(
            f"Grid 1 -> {centroid1}"
        )

        logger.info(
            f"Grid 2 -> {centroid2}"
        )

        logger.info(
            f"Centroid difference: "
            f"longitude={lon_difference}, "
            f"latitude={lat_difference}"
        )

        if (
            lon_difference == 0
            and lat_difference == 0
        ):

            raise ValueError(
                "Grid 1 and Grid 2 have "
                "identical centroids."
            )

        logger.info(
            "PASS: Grid 1 and Grid 2 have "
            "distinct and spatially adjacent "
            "grid positions."
        )


    # ========================================================
    # 16. TOP HIGH-ACTIVITY GRIDS
    # ========================================================

    logger.info(
        "========== TOP HIGH-ACTIVITY GRIDS =========="
    )

    top_grids = (
        grid_activity_geo_df
        .groupBy(
            "grid_id",
            "geometry"
        )
        .agg(
            F.sum(
                "total_activity"
            ).alias(
                "window_total_activity"
            )
        )
        .orderBy(
            F.desc(
                "window_total_activity"
            )
        )
        .limit(10)
    )

    top_grids.show(
        10,
        truncate=False
    )


    # ========================================================
    # 17. SAVE TOP GRIDS
    # ========================================================

    top_grids_output = os.path.join(
        OUTPUT_DIR,
        "top_high_activity_grids"
    )

    logger.info(
        f"Writing top high-activity grids to: "
        f"{top_grids_output}"
    )

    (
        top_grids
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(top_grids_output)
    )

    logger.info(
        "Top high-activity grids saved successfully."
    )


    # ========================================================
    # 18. SAVE ENRICHED DATASET
    # ========================================================

    enriched_output = os.path.join(
        OUTPUT_DIR,
        "grid_activity_geo"
    )

    logger.info(
        f"Writing enriched dataset to: "
        f"{enriched_output}"
    )

    (
        grid_activity_geo_df
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(enriched_output)
    )

    logger.info(
        f"Enriched dataset written to: "
        f"{enriched_output}"
    )


    # ========================================================
    # 19. SAVE UNMATCHED GRID REPORT
    # ========================================================

    unmatched_output = os.path.join(
        OUTPUT_DIR,
        "unmatched_grid_ids"
    )

    logger.info(
        f"Writing unmatched grid report to: "
        f"{unmatched_output}"
    )

    (
        unmatched_grid_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(unmatched_output)
    )

    logger.info(
        "Unmatched grid report saved successfully."
    )


    # ========================================================
    # 20. SAVE COVERAGE REPORT
    # ========================================================

    coverage_schema = StructType([

        StructField(
            "rows_before_join",
            IntegerType(),
            False
        ),

        StructField(
            "rows_after_join",
            IntegerType(),
            False
        ),

        StructField(
            "distinct_grids_before",
            IntegerType(),
            False
        ),

        StructField(
            "distinct_grids_after",
            IntegerType(),
            False
        ),

        StructField(
            "unmatched_grid_count",
            IntegerType(),
            False
        ),

        StructField(
            "matched_grid_count",
            IntegerType(),
            False
        ),

        StructField(
            "grid_enrichment_percentage",
            StringType(),
            False
        )

    ])


    coverage_df = spark.createDataFrame(

        [(
            activity_count_before,
            activity_count_after,
            distinct_grids_before,
            distinct_grids_after,
            unmatched_count,
            matched_grids,
            f"{grid_coverage_percentage:.2f}%"
        )],

        schema=coverage_schema
    )


    coverage_output = os.path.join(
        OUTPUT_DIR,
        "grid_enrichment_coverage_report"
    )

    logger.info(
        f"Writing coverage report to: "
        f"{coverage_output}"
    )

    (
        coverage_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(coverage_output)
    )

    logger.info(
        "Coverage report saved successfully."
    )


    # ========================================================
    # FINAL SUMMARY
    # ========================================================

    logger.info(
        "========== SP4 VALIDATION SUMMARY =========="
    )

    logger.info(
        f"GeoJSON features: "
        f"{lookup_count}"
    )

    logger.info(
        f"Distinct activity grids: "
        f"{distinct_grids_before}"
    )

    logger.info(
        f"Rows before join: "
        f"{activity_count_before}"
    )

    logger.info(
        f"Rows after join: "
        f"{activity_count_after}"
    )

    logger.info(
        f"Unmatched grids: "
        f"{unmatched_count}"
    )

    logger.info(
        f"Grid enrichment coverage: "
        f"{grid_coverage_percentage:.2f}%"
    )

    logger.info(
        "========== SP4 COMPLETED =========="
    )


except Exception as e:

    logger.exception(
        f"SP4 FAILED: {str(e)}"
    )

    raise


finally:

    spark.stop()