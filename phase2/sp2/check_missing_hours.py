from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, explode, sequence, expr

spark = (
    SparkSession.builder
    .appName("CheckMissingHours")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("../data/landing")
)

df = df.withColumn(
    "timestamp",
    col("datetime").cast("timestamp")
)

actual = (
    df.select("timestamp")
      .where(col("timestamp").isNotNull())
      .distinct()
)

bounds = actual.select(
    min("timestamp").alias("start"),
    max("timestamp").alias("end")
)

expected = bounds.select(
    explode(
        sequence(
            col("start"),
            col("end"),
            expr("INTERVAL 1 HOUR")
        )
    ).alias("timestamp")
)

missing = expected.join(
    actual,
    on="timestamp",
    how="left_anti"
)

print("\nMissing hourly timestamps:")
missing.orderBy("timestamp").show(20, False)

print(
    "Missing count:",
    missing.count()
)

spark.stop()
