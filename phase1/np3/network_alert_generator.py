import logging
from pathlib import Path

import pandas as pd


# =========================================================
# PATHS
# =========================================================

BASE_DIR = Path(__file__).resolve().parent

INPUT_FILE = (
    BASE_DIR.parent
    / "np2"
    / "grid_hour_analytics.csv"
)

OUTPUT_FILE = BASE_DIR / "network_alerts.csv"

LOG_FILE = BASE_DIR / "np3_execution.log"


# =========================================================
# APPROVED THRESHOLDS
# =========================================================

# Grids with daily activity below this value are ignored.
ACTIVITY_FLOOR = 500

# Current activity must be at least 2x the baseline.
HIGH_ACTIVITY_MULTIPLIER = 2.0

# Current activity must increase by at least 50%
# compared with the immediately preceding hour.
SPIKE_THRESHOLD = 0.50

# Current activity must be at or below 50% of baseline.
DROP_MULTIPLIER = 0.50


# =========================================================
# LOGGING SETUP
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# =========================================================
# 1. LOAD DATA
# =========================================================

def load_data():

    logger.info("========== NP3 START ==========")
    logger.info("Loading NP2 grid/hour analytics...")

    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}"
        )

    df = pd.read_csv(INPUT_FILE)

    required_columns = [
        "grid_id",
        "timestamp",
        "total_activity"
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    # -----------------------------------------------------
    # TIMESTAMP FIX
    # -----------------------------------------------------
    # NP2 timestamp values contain both date and time,
    # for example:
    #
    # 2013-11-01 00:00:00
    #
    # format="mixed" allows Pandas to correctly parse
    # the timestamp values.
    # -----------------------------------------------------

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        format="mixed"
    )

    logger.info(
        f"INPUT_ROWS={len(df)}"
    )

    logger.info(
        f"INPUT_COLUMNS={len(df.columns)}"
    )

    print("\n===== LOAD VALIDATION =====")
    print(f"Loaded rows: {len(df)}")
    print(f"Loaded columns: {len(df.columns)}")

    return df


# =========================================================
# 2. PREPARE DATA
# =========================================================

def prepare_data(df):

    logger.info(
        "Preparing grid/hour data..."
    )

    df = df.copy()

    # Convert activity to numeric
    df["total_activity"] = pd.to_numeric(
        df["total_activity"],
        errors="coerce"
    )

    # Reject invalid activity values
    if df["total_activity"].isna().any():
        raise ValueError(
            "total_activity contains null or invalid values."
        )

    # Sort by grid and timestamp
    df = df.sort_values(
        ["grid_id", "timestamp"]
    ).reset_index(drop=True)

    # -----------------------------------------------------
    # PREVIOUS HOUR
    # -----------------------------------------------------

    df["previous_activity"] = (
        df.groupby("grid_id")["total_activity"]
        .shift(1)
    )

    # -----------------------------------------------------
    # DAILY TOTAL
    # -----------------------------------------------------

    df["daily_total_activity"] = (
        df.groupby("grid_id")["total_activity"]
        .transform("sum")
    )

    logger.info(
        f"DISTINCT_GRIDS={df['grid_id'].nunique()}"
    )

    logger.info(
        f"DISTINCT_DATES={df['timestamp'].dt.date.nunique()}"
    )

    logger.info(
        f"DISTINCT_HOURS={df['timestamp'].dt.hour.nunique()}"
    )

    print("\n===== DATA PREPARATION =====")
    print(
        f"Distinct grids: {df['grid_id'].nunique()}"
    )
    print(
        f"Distinct dates: {df['timestamp'].dt.date.nunique()}"
    )
    print(
        f"Distinct hours: {df['timestamp'].dt.hour.nunique()}"
    )

    return df


# =========================================================
# 3. CALCULATE WITHIN-DAY BASELINE
# =========================================================

def calculate_within_day_baseline(df):

    logger.info(
        "Calculating within-day baselines..."
    )

    df = df.copy()

    baseline_values = []

    # -----------------------------------------------------
    # IMPORTANT:
    #
    # For every grid/hour, the current hour is excluded.
    #
    # Example:
    #
    # Grid 1, 05:00
    #
    # Current value = 05:00 activity
    #
    # Baseline = median of the OTHER 23 hourly values.
    # -----------------------------------------------------

    for grid_id, group in df.groupby(
        "grid_id",
        sort=False
    ):

        group = group.sort_values(
            "timestamp"
        )

        values = group[
            "total_activity"
        ].tolist()

        for i in range(len(values)):

            # Exclude the current hour
            other_values = (
                values[:i] +
                values[i + 1:]
            )

            if len(other_values) == 0:

                baseline_values.append(
                    float("nan")
                )

            else:

                baseline_values.append(
                    float(
                        pd.Series(
                            other_values
                        ).median()
                    )
                )

    df["baseline_activity"] = baseline_values

    logger.info(
        "Within-day baseline calculation completed."
    )

    print(
        "\n===== BASELINE VALIDATION ====="
    )

    print(
        "Baseline type: Within-day median"
    )

    print(
        "Hours used per baseline: 23"
    )

    print(
        "Current hour excluded: YES"
    )

    return df


# =========================================================
# 4. GENERATE ALERTS
# =========================================================

def generate_alerts(df):

    logger.info(
        "Applying NP3 alert rules..."
    )

    alerts = []

    for _, row in df.iterrows():

        grid_id = row["grid_id"]

        timestamp = row["timestamp"]

        current = row["total_activity"]

        baseline = row["baseline_activity"]

        previous = row["previous_activity"]

        daily_total = row[
            "daily_total_activity"
        ]

        # -------------------------------------------------
        # ACTIVITY FLOOR
        # -------------------------------------------------

        if daily_total < ACTIVITY_FLOOR:
            continue

        # Baseline must exist
        if pd.isna(baseline):
            continue

        # -------------------------------------------------
        # RULE 1: HIGH_ACTIVITY
        # -------------------------------------------------

        if current >= (
            HIGH_ACTIVITY_MULTIPLIER
            * baseline
        ):

            alerts.append({

                "grid_id": grid_id,

                "timestamp": timestamp,

                "alert_type": "HIGH_ACTIVITY",

                "current_activity": current,

                "baseline_activity": baseline,

                "reason": (
                    f"HIGH_ACTIVITY: current activity "
                    f"{current:.2f} is at least "
                    f"{HIGH_ACTIVITY_MULTIPLIER:.1f}x "
                    f"the within-day baseline "
                    f"{baseline:.2f}."
                )
            })

        # -------------------------------------------------
        # RULE 2: ACTIVITY_SPIKE
        # -------------------------------------------------

        if (
            not pd.isna(previous)
            and previous > 0
        ):

            change = (
                (current - previous)
                / previous
            )

            if change >= SPIKE_THRESHOLD:

                alerts.append({

                    "grid_id": grid_id,

                    "timestamp": timestamp,

                    "alert_type": "ACTIVITY_SPIKE",

                    "current_activity": current,

                    "baseline_activity": baseline,

                    "reason": (
                        f"ACTIVITY_SPIKE: current activity "
                        f"{current:.2f} increased by "
                        f"{change * 100:.1f}% from "
                        f"the previous hour "
                        f"{previous:.2f}."
                    )
                })

        # -------------------------------------------------
        # RULE 3: ACTIVITY_DROP
        # -------------------------------------------------

        if current <= (
            DROP_MULTIPLIER
            * baseline
        ):

            alerts.append({

                "grid_id": grid_id,

                "timestamp": timestamp,

                "alert_type": "ACTIVITY_DROP",

                "current_activity": current,

                "baseline_activity": baseline,

                "reason": (
                    f"ACTIVITY_DROP: current activity "
                    f"{current:.2f} is at or below "
                    f"{DROP_MULTIPLIER:.1f}x "
                    f"the within-day baseline "
                    f"{baseline:.2f}."
                )
            })

    alerts_df = pd.DataFrame(
        alerts,
        columns=[
            "grid_id",
            "timestamp",
            "alert_type",
            "current_activity",
            "baseline_activity",
            "reason"
        ]
    )

    logger.info(
        f"TOTAL_ALERT_RECORDS={len(alerts_df)}"
    )

    return alerts_df


# =========================================================
# 5. EXPORT ALERTS
# =========================================================

def export_alerts(alerts_df):

    logger.info(
        "Exporting network alerts..."
    )

    alerts_df.to_csv(
        OUTPUT_FILE,
        index=False
    )

    logger.info(
        f"ALERT_FILE={OUTPUT_FILE}"
    )

    print("\n===== EXPORTED FILE =====")

    print(
        f"Network alerts: {OUTPUT_FILE}"
    )


# =========================================================
# 6. OPERATIONAL SUMMARY
# =========================================================

def print_summary(df, alerts_df):

    total_grid_hours = len(df)

    total_alerts = len(alerts_df)

    if total_grid_hours > 0:

        alert_proportion = (
            total_alerts
            / total_grid_hours
        )

    else:

        alert_proportion = 0

    print("\n" + "=" * 60)

    print(
        "NP3 RULE-BASED NETWORK ACTIVITY ALERT SUMMARY"
    )

    print("=" * 60)

    print(
        f"Total grid/hours checked : "
        f"{total_grid_hours}"
    )

    print(
        f"Total alert records      : "
        f"{total_alerts}"
    )

    print(
        f"Alert proportion         : "
        f"{alert_proportion * 100:.2f}%"
    )

    # -----------------------------------------------------
    # THRESHOLDS
    # -----------------------------------------------------

    print("\n===== APPROVED THRESHOLDS =====")

    print(
        f"Activity floor           : "
        f"{ACTIVITY_FLOOR}"
    )

    print(
        f"HIGH_ACTIVITY            : "
        f"{HIGH_ACTIVITY_MULTIPLIER:.1f}x baseline"
    )

    print(
        f"ACTIVITY_SPIKE           : "
        f"{SPIKE_THRESHOLD * 100:.0f}% increase"
    )

    print(
        f"ACTIVITY_DROP            : "
        f"{(1 - DROP_MULTIPLIER) * 100:.0f}% below baseline"
    )

    # -----------------------------------------------------
    # ALERTS BY TYPE
    # -----------------------------------------------------

    print("\n===== ALERTS BY TYPE =====")

    if total_alerts == 0:

        print("No alerts generated.")

    else:

        alert_counts = (
            alerts_df[
                "alert_type"
            ]
            .value_counts()
        )

        for alert_type, count in (
            alert_counts.items()
        ):

            print(
                f"{alert_type:20s}: {count}"
            )

    # -----------------------------------------------------
    # TOP 10 GRIDS
    # -----------------------------------------------------

    print("\n===== TOP 10 GRIDS BY ALERT COUNT =====")

    if total_alerts == 0:

        print("No alerting grids.")

    else:

        top_grids = (
            alerts_df[
                "grid_id"
            ]
            .value_counts()
            .head(10)
        )

        for grid_id, count in (
            top_grids.items()
        ):

            print(
                f"Grid {int(grid_id):5d}: "
                f"{count} alerts"
            )

    print("=" * 60)


# =========================================================
# 7. ACCEPTANCE TESTS
# =========================================================

def run_acceptance_tests(
    df,
    alerts_df
):

    logger.info(
        "========== NP3 ACCEPTANCE TESTS START =========="
    )

    # -----------------------------------------------------
    # TEST 1: INPUT ROW COUNT
    # -----------------------------------------------------

    assert len(df) == 240000, (
        f"Expected 240000 rows, "
        f"got {len(df)}"
    )

    logger.info(
        "VALIDATION input_rows: PASS"
    )

    # -----------------------------------------------------
    # TEST 2: ONE DAY
    # -----------------------------------------------------

    assert (
        df["timestamp"]
        .dt.date
        .nunique()
        == 1
    )

    assert (
        df["timestamp"]
        .dt.hour
        .nunique()
        == 24
    )

    logger.info(
        "VALIDATION one_day_24_hours: PASS"
    )

    # -----------------------------------------------------
    # TEST 3: GRID COUNT
    # -----------------------------------------------------

    assert (
        df["grid_id"]
        .nunique()
        == 10000
    )

    logger.info(
        "VALIDATION grid_count: PASS"
    )

    # -----------------------------------------------------
    # TEST 4: DUPLICATES
    # -----------------------------------------------------

    duplicates = (
        df.duplicated(
            subset=[
                "grid_id",
                "timestamp"
            ]
        )
        .sum()
    )

    assert duplicates == 0

    logger.info(
        "VALIDATION grid_hour_duplicates: PASS"
    )

    # -----------------------------------------------------
    # TEST 5:
    # BASELINE EXCLUDES CURRENT HOUR
    # -----------------------------------------------------

    first_grid = (
        df["grid_id"]
        .iloc[0]
    )

    sample = (
        df[
            df["grid_id"]
            == first_grid
        ]
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    sample_hour_index = 5

    current_value = (
        sample.loc[
            sample_hour_index,
            "total_activity"
        ]
    )

    expected_baseline = (
        sample.drop(
            index=sample_hour_index
        )[
            "total_activity"
        ]
        .median()
    )

    actual_baseline = (
        sample.loc[
            sample_hour_index,
            "baseline_activity"
        ]
    )

    assert abs(
        actual_baseline
        - expected_baseline
    ) < 1e-9

    other_values = (
        sample.drop(
            index=sample_hour_index
        )[
            "total_activity"
        ]
    )

    assert len(other_values) == 23

    logger.info(
        "VALIDATION baseline_excludes_current_hour: PASS"
    )

    # -----------------------------------------------------
    # TEST 6: ALERT COLUMNS
    # -----------------------------------------------------

    expected_columns = [
        "grid_id",
        "timestamp",
        "alert_type",
        "current_activity",
        "baseline_activity",
        "reason"
    ]

    assert (
        list(alerts_df.columns)
        == expected_columns
    )

    logger.info(
        "VALIDATION alert_columns: PASS"
    )

    # -----------------------------------------------------
    # TEST 7: ALERT TYPES
    # -----------------------------------------------------

    valid_alert_types = {
        "HIGH_ACTIVITY",
        "ACTIVITY_SPIKE",
        "ACTIVITY_DROP"
    }

    if len(alerts_df) > 0:

        assert set(
            alerts_df[
                "alert_type"
            ].unique()
        ).issubset(
            valid_alert_types
        )

    logger.info(
        "VALIDATION alert_types: PASS"
    )

    # -----------------------------------------------------
    # TEST 8: HUMAN-READABLE REASONS
    # -----------------------------------------------------

    if len(alerts_df) > 0:

        assert (
            alerts_df[
                "reason"
            ]
            .notna()
            .all()
        )

        assert (
            alerts_df[
                "reason"
            ]
            .str.len()
            .gt(0)
            .all()
        )

    logger.info(
        "VALIDATION alert_reasons: PASS"
    )

    # -----------------------------------------------------
    # TEST 9: GRID IDS EXIST
    # -----------------------------------------------------

    input_grids = set(
        df["grid_id"]
        .unique()
    )

    alert_grids = set(
        alerts_df["grid_id"]
        .unique()
    )

    assert alert_grids.issubset(
        input_grids
    )

    logger.info(
        "VALIDATION alert_grid_ids: PASS"
    )

    # -----------------------------------------------------
    # TEST 10: ACTIVITY FLOOR
    # -----------------------------------------------------

    if len(alerts_df) > 0:

        daily_totals = (
            df.groupby("grid_id")
            ["total_activity"]
            .sum()
        )

        for grid_id in alert_grids:

            assert (
                daily_totals.loc[grid_id]
                >= ACTIVITY_FLOOR
            )

    logger.info(
        "VALIDATION activity_floor: PASS"
    )

    # -----------------------------------------------------
    # TEST 11: OPERATIONAL ALERT VOLUME
    # -----------------------------------------------------

    total_grid_hours = len(df)

    alert_proportion = (
        len(alerts_df)
        / total_grid_hours
    )

    # More than 25% of all grid-hours
    # would be considered too high for
    # this training exercise.
    #
    # If this fails, thresholds should
    # be reviewed before sign-off.

    assert alert_proportion <= 0.25, (
        f"Alert proportion too high: "
        f"{alert_proportion * 100:.2f}%"
    )

    logger.info(
        "VALIDATION operational_alert_volume: PASS"
    )

    # -----------------------------------------------------
    # FINAL ACCEPTANCE
    # -----------------------------------------------------

    logger.info(
        "========== NP3 ACCEPTANCE: PASS =========="
    )

    print(
        "\n===== NP3 ACCEPTANCE: PASS ====="
    )

    print(
        "All NP3 acceptance tests passed."
    )

    print(
        f"Execution log: {LOG_FILE}"
    )


# =========================================================
# 8. MAIN
# =========================================================

def main():

    try:

        # -------------------------------------------------
        # STEP 1
        # -------------------------------------------------

        df = load_data()

        # -------------------------------------------------
        # STEP 2
        # -------------------------------------------------

        df = prepare_data(df)

        # -------------------------------------------------
        # STEP 3
        # -------------------------------------------------

        df = calculate_within_day_baseline(
            df
        )

        # -------------------------------------------------
        # STEP 4
        # -------------------------------------------------

        alerts_df = generate_alerts(
            df
        )

        # -------------------------------------------------
        # STEP 5
        # -------------------------------------------------

        export_alerts(
            alerts_df
        )

        # -------------------------------------------------
        # STEP 6
        # -------------------------------------------------

        print_summary(
            df,
            alerts_df
        )

        # -------------------------------------------------
        # STEP 7
        # -------------------------------------------------

        run_acceptance_tests(
            df,
            alerts_df
        )

    except Exception as e:

        logger.exception(
            f"NP3 FAILED: {e}"
        )

        print(
            "\n===== NP3 ACCEPTANCE: FAIL ====="
        )

        print(
            f"Error: {e}"
        )

        raise


# =========================================================
# PROGRAM ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()