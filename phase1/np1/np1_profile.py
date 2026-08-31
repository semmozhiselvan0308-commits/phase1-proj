import pandas as pd
from pathlib import Path


# ============================================================
# NP1 - PROFILE THE TELECOM ACTIVITY DATASET
# ============================================================

print("========== NP1 START ==========")


# ------------------------------------------------------------
# 1. Project paths
# ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

LANDING_DIR = BASE_DIR / "data" / "landing"
REFERENCE_DIR = BASE_DIR / "data" / "reference"

# Use ONE daily CSV for NP1
FILE_PATH = LANDING_DIR / "sms-call-internet-mi-2013-11-01.csv"


# ------------------------------------------------------------
# 2. Load raw CSV
# ------------------------------------------------------------

print("\n========== LOADING RAW DATA ==========")

print("File:")
print(FILE_PATH)

df = pd.read_csv(FILE_PATH)

print("\nFile loaded successfully.")


# ------------------------------------------------------------
# 3. Raw dataset profile
# ------------------------------------------------------------

print("\n========== RAW DATASET PROFILE ==========")

print("Shape:")
print(df.shape)

print("\nRaw Columns:")
print(df.columns.tolist())

print("\nRaw Data Types:")
print(df.dtypes)


# ------------------------------------------------------------
# 4. First 5 rows
# ------------------------------------------------------------

print("\n========== FIRST 5 ROWS ==========")

print(df.head())


# ------------------------------------------------------------
# 5. Raw missing-value check
# ------------------------------------------------------------

print("\n========== MISSING VALUES ==========")

missing_values = df.isnull().sum()

print(missing_values)


# ------------------------------------------------------------
# 6. Exact duplicate check
# ------------------------------------------------------------

print("\n========== DUPLICATES ==========")

duplicate_count = df.duplicated().sum()

print("Exact duplicate rows:", duplicate_count)


# ------------------------------------------------------------
# 7. Unique values
# ------------------------------------------------------------

print("\n========== UNIQUE VALUES ==========")

for column in df.columns:
    print(
        f"{column}: {df[column].nunique()} unique values"
    )


# ------------------------------------------------------------
# 8. Canonical column mapping
# ------------------------------------------------------------

print("\n========== CANONICAL COLUMN MAPPING ==========")

column_mapping = {
    "datetime": "timestamp",
    "CellID": "grid_id",
    "countrycode": "country_code",
    "smsin": "sms_in",
    "smsout": "sms_out",
    "callin": "call_in",
    "callout": "call_out",
    "internet": "internet"
}

print("Mapping:")

for raw_column, canonical_column in column_mapping.items():
    print(
        f"{raw_column} -> {canonical_column}"
    )

df = df.rename(columns=column_mapping)


# ------------------------------------------------------------
# 9. Display canonical columns
# ------------------------------------------------------------

print("\n========== CANONICAL COLUMNS ==========")

print(df.columns.tolist())


# ------------------------------------------------------------
# 10. Convert timestamp
# ------------------------------------------------------------

print("\n========== TIMESTAMP CONVERSION ==========")

df["timestamp"] = pd.to_datetime(
    df["timestamp"],
    errors="coerce"
)

print("Timestamp data type:")
print(df["timestamp"].dtype)

invalid_timestamp_count = df["timestamp"].isnull().sum()

print(
    "Invalid timestamp values:",
    invalid_timestamp_count
)


# ------------------------------------------------------------
# 11. Time range
# ------------------------------------------------------------

print("\n========== TIME RANGE ==========")

print(
    "Minimum timestamp:",
    df["timestamp"].min()
)

print(
    "Maximum timestamp:",
    df["timestamp"].max()
)


# ------------------------------------------------------------
# 12. Distinct timestamp check
# ------------------------------------------------------------

print("\n========== TIMESTAMP CHECK ==========")

unique_timestamps = (
    df["timestamp"]
    .dropna()
    .drop_duplicates()
    .sort_values()
)

print(
    "Number of distinct timestamps:",
    len(unique_timestamps)
)

print("\nAll distinct timestamps:")

for timestamp in unique_timestamps:
    print(timestamp)


# ------------------------------------------------------------
# 13. Hourly cadence validation
# ------------------------------------------------------------

print("\n========== CADENCE CHECK ==========")

time_difference = (
    unique_timestamps.diff()
    .dropna()
)

print("Time differences between consecutive timestamps:")

print(time_difference.value_counts())


# Check whether every interval is exactly one hour
all_intervals_one_hour = (
    len(time_difference) > 0
    and
    (time_difference == pd.Timedelta(hours=1)).all()
)

exactly_24_timestamps = (
    len(unique_timestamps) == 24
)

hourly_cadence_valid = (
    exactly_24_timestamps
    and all_intervals_one_hour
)

print(
    "\nExactly 24 timestamps:",
    exactly_24_timestamps
)

print(
    "All intervals are 1 hour:",
    all_intervals_one_hour
)

print(
    "Hourly cadence valid:",
    hourly_cadence_valid
)


# ------------------------------------------------------------
# 14. Derive date, hour and day of week
# ------------------------------------------------------------

print("\n========== TIME-DERIVED FIELDS ==========")

df["date"] = df["timestamp"].dt.date

df["hour"] = df["timestamp"].dt.hour

df["day_of_week"] = (
    df["timestamp"].dt.day_name()
)

print(
    df[
        [
            "timestamp",
            "date",
            "hour",
            "day_of_week"
        ]
    ].head(10)
)


# ------------------------------------------------------------
# 15. Check missing grid and timestamp
# ------------------------------------------------------------

print("\n========== KEY FIELD NULL CHECK ==========")

print(
    "Missing grid_id:",
    df["grid_id"].isnull().sum()
)

print(
    "Missing timestamp:",
    df["timestamp"].isnull().sum()
)


# ------------------------------------------------------------
# 16. Blank activity measures
# ------------------------------------------------------------

print("\n========== ACTIVITY NULL CHECK ==========")

activity_columns = [
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet"
]

for column in activity_columns:

    null_count = df[column].isnull().sum()

    print(
        f"{column}: {null_count} null values"
    )


# ------------------------------------------------------------
# 17. Negative activity values
# ------------------------------------------------------------

print("\n========== NEGATIVE ACTIVITY CHECK ==========")

for column in activity_columns:

    negative_count = (
        df[column] < 0
    ).sum()

    print(
        f"{column}: {negative_count} negative values"
    )


# ------------------------------------------------------------
# 18. Unique grids
# ------------------------------------------------------------

print("\n========== GRID INFORMATION ==========")

print(
    "Unique grids:",
    df["grid_id"].nunique()
)


# ------------------------------------------------------------
# 19. Country-code categories
# ------------------------------------------------------------

print("\n========== COUNTRY CODE INFORMATION ==========")

country_codes = (
    df["country_code"]
    .dropna()
    .unique()
)

print(
    "Number of country-code categories:",
    len(country_codes)
)

print(
    "Country-code categories:"
)

print(
    sorted(country_codes)
)


# ------------------------------------------------------------
# 20. Raw grain validation
# ------------------------------------------------------------

print("\n========== RAW GRAIN CHECK ==========")

grain_columns = [
    "timestamp",
    "grid_id",
    "country_code"
]

grain_duplicates = df.duplicated(
    subset=grain_columns
).sum()

print(
    "Duplicate timestamp + grid_id + country_code records:",
    grain_duplicates
)

if grain_duplicates == 0:

    print(
        "Raw grain is unique at:"
    )

    print(
        "timestamp + grid_id + country_code"
    )

else:

    print(
        "WARNING: Duplicate records exist at the expected raw grain."
    )


# ------------------------------------------------------------
# 21. Create derived activity measures
# ------------------------------------------------------------

print("\n========== DERIVED ACTIVITY MEASURES ==========")

df["total_sms"] = (
    df["sms_in"].fillna(0)
    +
    df["sms_out"].fillna(0)
)

df["total_calls"] = (
    df["call_in"].fillna(0)
    +
    df["call_out"].fillna(0)
)

df["total_activity"] = (
    df["total_sms"]
    +
    df["total_calls"]
    +
    df["internet"].fillna(0)
)


print(
    df[
        [
            "total_sms",
            "total_calls",
            "total_activity"
        ]
    ].head()
)


# ------------------------------------------------------------
# 22. Busiest hourly window
# ------------------------------------------------------------

print("\n========== BUSIEST HOUR ==========")

hourly_activity = (
    df.groupby("hour")["total_activity"]
    .sum()
    .sort_values(ascending=False)
)

print(
    hourly_activity.head(10)
)

busiest_hour = hourly_activity.idxmax()

busiest_hour_value = hourly_activity.max()

print(
    "\nBusiest hour:",
    busiest_hour
)

print(
    "Total activity:",
    busiest_hour_value
)


# ------------------------------------------------------------
# 23. Busiest grid
# ------------------------------------------------------------

print("\n========== BUSIEST GRID ==========")

grid_activity = (
    df.groupby("grid_id")["total_activity"]
    .sum()
    .sort_values(ascending=False)
)

print(
    grid_activity.head(10)
)

busiest_grid = grid_activity.idxmax()

busiest_grid_value = grid_activity.max()

print(
    "\nBusiest grid:",
    busiest_grid
)

print(
    "Total activity:",
    busiest_grid_value
)


# ------------------------------------------------------------
# 24. Final profiling facts
# ------------------------------------------------------------

print("\n========== FINAL PROFILING FACTS ==========")

print(
    "Total rows:",
    len(df)
)

print(
    "Unique grids:",
    df["grid_id"].nunique()
)

print(
    "Unique country codes:",
    df["country_code"].nunique()
)

print(
    "Distinct timestamps:",
    len(unique_timestamps)
)

print(
    "Time range:",
    df["timestamp"].min(),
    "to",
    df["timestamp"].max()
)

print(
    "Hourly cadence valid:",
    hourly_cadence_valid
)

print(
    "Exact duplicates:",
    duplicate_count
)

print(
    "Busiest hour:",
    busiest_hour
)

print(
    "Busiest grid:",
    busiest_grid
)


# ------------------------------------------------------------
# 25. Final canonical DataFrame preview
# ------------------------------------------------------------

print("\n========== CANONICAL DATA PREVIEW ==========")

canonical_columns = [
    "timestamp",
    "grid_id",
    "country_code",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet",
    "date",
    "hour",
    "day_of_week",
    "total_sms",
    "total_calls",
    "total_activity"
]

print(
    df[canonical_columns].head(10)
)


# ------------------------------------------------------------
# 26. End
# ------------------------------------------------------------

print("\n========== NP1 COMPLETE ==========")