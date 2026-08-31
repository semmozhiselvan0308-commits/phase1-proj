import logging
import os
import pandas as pd


# =========================================================
# LOGGING SETUP
# =========================================================

LOG_FILE = "np2_execution.log"

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
# USAGE PROCESSOR
# =========================================================

class UsageProcessor:

    def __init__(self, file_path=None, df=None):

        self.file_path = file_path
        self.raw_data = df
        self.cleaned_data = None
        self.grid_time_data = None
        self.daily_summary = None
        self.grid_summary = None

        self.input_rows = 0
        self.rejected_rows = 0
        self.nulls_handled = 0
        self.output_rows = 0

    # =====================================================
    # 1. LOAD DATA
    # =====================================================

    def load_data(self):

        if self.raw_data is not None:

            logger.info("DataFrame provided directly.")

            self.input_rows = len(self.raw_data)

            logger.info(
                "INPUT_ROWS=%s",
                self.input_rows
            )

            return self.raw_data.copy()

        if self.file_path is None:

            raise ValueError(
                "Either file_path or df must be provided."
            )

        self.raw_data = pd.read_csv(
            self.file_path
        )

        self.input_rows = len(
            self.raw_data
        )

        logger.info(
            "Data loaded successfully."
        )

        logger.info(
            "INPUT_ROWS=%s",
            self.input_rows
        )

        return self.raw_data.copy()

    # =====================================================
    # 2. CLEAN DATA
    # =====================================================

    def clean_data(self):

        if self.raw_data is None:

            raise ValueError(
                "No data loaded. Run load_data() first."
            )

        df = self.raw_data.copy()

        input_rows = len(df)

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

        df = df.rename(
            columns=column_mapping
        )

        required_columns = [

            "timestamp",
            "grid_id",
            "country_code",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet"

        ]

        missing_columns = [

            col
            for col in required_columns
            if col not in df.columns

        ]

        if missing_columns:

            raise ValueError(
                f"Missing required columns: "
                f"{missing_columns}"
            )

        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce"
        )

        invalid_key_rows = (

            df["grid_id"].isna()
            |
            df["timestamp"].isna()

        )

        rejected_rows = int(
            invalid_key_rows.sum()
        )

        if rejected_rows > 0:

            df = df.loc[
                ~invalid_key_rows
            ].copy()

        self.rejected_rows = rejected_rows

        activity_columns = [

            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet"

        ]

        negative_counts = (

            df[activity_columns]
            .lt(0)
            .sum()

        )

        total_negative_values = int(
            negative_counts.sum()
        )

        if total_negative_values > 0:

            raise ValueError(
                "Negative activity values found: "
                f"{negative_counts.to_dict()}"
            )

        nulls_handled = int(

            df[activity_columns]
            .isna()
            .sum()
            .sum()

        )

        df[activity_columns] = (

            df[activity_columns]
            .fillna(0)

        )

        self.nulls_handled = nulls_handled

        self.cleaned_data = df

        self.output_rows = len(
            self.cleaned_data
        )

        logger.info(
            "INPUT_ROWS=%s",
            input_rows
        )

        logger.info(
            "ROWS_REJECTED=%s",
            self.rejected_rows
        )

        logger.info(
            "NULLS_HANDLED_CURATED_RULE=%s",
            self.nulls_handled
        )

        logger.info(
            "CLEANED_OUTPUT_ROWS=%s",
            self.output_rows
        )

        return self.cleaned_data.copy()

    # =====================================================
    # 3. DERIVE TIME FEATURES
    # =====================================================

    def derive_time_features(self):

        if self.cleaned_data is None:

            raise ValueError(
                "No cleaned data available. "
                "Run clean_data() first."
            )

        df = self.cleaned_data.copy()

        df["date"] = (
            df["timestamp"].dt.date
        )

        df["hour"] = (
            df["timestamp"].dt.hour
        )

        df["day_of_week"] = (
            df["timestamp"].dt.day_name()
        )

        self.cleaned_data = df

        logger.info(
            "Time features derived successfully."
        )

        logger.info(
            "Distinct dates: %s",
            df["date"].nunique()
        )

        logger.info(
            "Distinct hours: %s",
            df["hour"].nunique()
        )

        return self.cleaned_data.copy()

    # =====================================================
    # 4. AGGREGATE TO GRID/HOUR
    # =====================================================

    def aggregate_to_grid_time(self):

        if self.cleaned_data is None:

            raise ValueError(
                "No cleaned data available. "
                "Run clean_data() first."
            )

        df = self.cleaned_data.copy()

        input_rows = len(df)

        activity_columns = [

            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet"

        ]

        grid_time = (

            df.groupby(
                [
                    "timestamp",
                    "grid_id"
                ],
                as_index=False
            )[activity_columns]
            .sum()

        )

        grid_time["date"] = (
            grid_time["timestamp"].dt.date
        )

        grid_time["hour"] = (
            grid_time["timestamp"].dt.hour
        )

        grid_time["day_of_week"] = (
            grid_time["timestamp"]
            .dt.day_name()
        )

        duplicate_count = int(

            grid_time.duplicated(
                subset=[
                    "grid_id",
                    "timestamp"
                ]
            ).sum()

        )

        if duplicate_count > 0:

            raise ValueError(
                "Duplicate grid/hour records found: "
                f"{duplicate_count}"
            )

        self.grid_time_data = grid_time

        self.output_rows = len(
            grid_time
        )

        logger.info(
            "Grid/hour aggregation completed."
        )

        logger.info(
            "AGGREGATION_INPUT_ROWS=%s",
            input_rows
        )

        logger.info(
            "AGGREGATION_OUTPUT_ROWS=%s",
            len(grid_time)
        )

        logger.info(
            "GRID_HOUR_DUPLICATES=%s",
            duplicate_count
        )

        return self.grid_time_data.copy()

    # =====================================================
    # 5. DERIVE ACTIVITY FEATURES
    # =====================================================

    def derive_activity_features(self):

        if self.grid_time_data is None:

            raise ValueError(
                "No grid/hour data available. "
                "Run aggregate_to_grid_time() first."
            )

        df = self.grid_time_data.copy()

        df["total_sms"] = (

            df["sms_in"]
            +
            df["sms_out"]

        )

        df["total_calls"] = (

            df["call_in"]
            +
            df["call_out"]

        )

        df["total_activity"] = (

            df["total_sms"]
            +
            df["total_calls"]
            +
            df["internet"]

        )

        self.grid_time_data = df

        logger.info(
            "Activity features derived successfully."
        )

        logger.info(
            "Grid/hour output rows: %s",
            len(df)
        )

        return self.grid_time_data.copy()

    # =====================================================
    # 6. COMPUTE KPIs
    # =====================================================

    def compute_kpis(self):

        if self.grid_time_data is None:

            raise ValueError(
                "No grid/hour data available. "
                "Run aggregate_to_grid_time() and "
                "derive_activity_features() first."
            )

        df = self.grid_time_data.copy()

        # -------------------------------------------------
        # DAILY SUMMARY
        # -------------------------------------------------

        daily_summary = (

            df.groupby("date")
            .agg(

                total_sms=(
                    "total_sms",
                    "sum"
                ),

                total_calls=(
                    "total_calls",
                    "sum"
                ),

                total_internet=(
                    "internet",
                    "sum"
                ),

                total_activity=(
                    "total_activity",
                    "sum"
                ),

                avg_activity_per_grid_hour=(
                    "total_activity",
                    "mean"
                ),

                active_grids=(
                    "grid_id",
                    "nunique"
                )

            )
            .reset_index()

        )

        # -------------------------------------------------
        # GRID SUMMARY
        # -------------------------------------------------

        grid_summary = (

            df.groupby("grid_id")
            .agg(

                total_sms=(
                    "total_sms",
                    "sum"
                ),

                total_calls=(
                    "total_calls",
                    "sum"
                ),

                total_internet=(
                    "internet",
                    "sum"
                ),

                total_activity=(
                    "total_activity",
                    "sum"
                ),

                avg_activity=(
                    "total_activity",
                    "mean"
                ),

                active_hours=(
                    "hour",
                    "nunique"
                )

            )
            .reset_index()

        )

        self.daily_summary = daily_summary

        self.grid_summary = grid_summary

        logger.info(
            "KPI computation completed."
        )

        logger.info(
            "Daily summary rows: %s",
            len(daily_summary)
        )

        logger.info(
            "Grid summary rows: %s",
            len(grid_summary)
        )

        return {

            "daily_summary":
                self.daily_summary.copy(),

            "grid_summary":
                self.grid_summary.copy()

        }

    # =========================================================
    # 7. EXPORT SUMMARY
    # =========================================================

    def export_summary(self, output_dir="."):

        if (
            self.daily_summary is None
            or
            self.grid_summary is None
            or
            self.grid_time_data is None
        ):
            raise ValueError(
                "Required outputs are not available. "
                "Run compute_kpis() and grid/hour processing first."
            )

        os.makedirs(
            output_dir,
            exist_ok=True
        )

        daily_path = os.path.join(
            output_dir,
            "daily_summary.csv"
        )

        grid_path = os.path.join(
            output_dir,
            "grid_summary.csv"
        )

        # NP3 INPUT
        grid_hour_path = os.path.join(
            output_dir,
            "grid_hour_analytics.csv"
        )

        # Existing outputs
        self.daily_summary.to_csv(
            daily_path,
            index=False
        )

        self.grid_summary.to_csv(
            grid_path,
            index=False
        )

        # New NP3 input
        self.grid_time_data.to_csv(
            grid_hour_path,
            index=False
        )

        logger.info(
            "Daily summary exported to: %s",
            os.path.abspath(daily_path)
        )

        logger.info(
            "Grid summary exported to: %s",
            os.path.abspath(grid_path)
        )

        logger.info(
            "Grid/hour analytics exported to: %s",
            os.path.abspath(grid_hour_path)
        )

        return {
            "daily_summary_path":
                daily_path,

            "grid_summary_path":
                grid_path,

            "grid_hour_analytics_path":
                grid_hour_path
        }


# =========================================================
# ACCEPTANCE TESTS
# =========================================================

def run_acceptance_tests(
    processor,
    loaded_data,
    cleaned_data,
    time_data,
    grid_time_data,
    activity_data,
    kpis,
    output_paths
):

    logger.info(
        "========== NP2 ACCEPTANCE TESTS START =========="
    )

    # -----------------------------------------------------
    # TEST 1 - load_data()
    # -----------------------------------------------------

    assert loaded_data is not None
    assert len(loaded_data) > 0

    logger.info(
        "VALIDATION load_data: PASS"
    )

    # -----------------------------------------------------
    # TEST 2 - clean_data()
    # -----------------------------------------------------

    assert cleaned_data is not None

    assert (
        processor.rejected_rows >= 0
    )

    assert (
        processor.nulls_handled >= 0
    )

    logger.info(
        "VALIDATION clean_data: PASS"
    )

    # -----------------------------------------------------
    # TEST 3 - derive_time_features()
    # -----------------------------------------------------

    assert "date" in time_data.columns
    assert "hour" in time_data.columns
    assert "day_of_week" in time_data.columns

    logger.info(
        "VALIDATION derive_time_features: PASS"
    )

    # -----------------------------------------------------
    # TEST 4 - aggregate_to_grid_time()
    # -----------------------------------------------------

    duplicates = int(

        grid_time_data.duplicated(
            subset=[
                "grid_id",
                "timestamp"
            ]
        ).sum()

    )

    assert duplicates == 0

    assert len(grid_time_data) < len(time_data)

    assert "country_code" not in (
        grid_time_data.columns
    )

    logger.info(
        "VALIDATION aggregate_to_grid_time: PASS"
    )

    # -----------------------------------------------------
    # TEST 5 - derive_activity_features()
    # -----------------------------------------------------

    required_activity_features = [

        "total_sms",
        "total_calls",
        "total_activity"

    ]

    for column in required_activity_features:

        assert column in activity_data.columns

    logger.info(
        "VALIDATION derive_activity_features: PASS"
    )

    # -----------------------------------------------------
    # TEST 6 - compute_kpis()
    # -----------------------------------------------------

    assert kpis is not None

    assert (
        "daily_summary"
        in kpis
    )

    assert (
        "grid_summary"
        in kpis
    )

    assert (
        len(kpis["daily_summary"]) > 0
    )

    assert (
        len(kpis["grid_summary"]) > 0
    )

    logger.info(
        "VALIDATION compute_kpis: PASS"
    )

    # -----------------------------------------------------
    # TEST 7 - export_summary()
    # -----------------------------------------------------

    assert os.path.exists(
        output_paths[
            "daily_summary_path"
        ]
    )

    assert os.path.exists(
        output_paths[
            "grid_summary_path"
        ]
    )

    assert os.path.exists(
        output_paths[
            "grid_hour_analytics_path"
        ]
    )

    # Verify NP3 input
    grid_hour_file = pd.read_csv(
        output_paths[
            "grid_hour_analytics_path"
        ]
    )

    assert len(grid_hour_file) == len(
        grid_time_data
    )

    assert "grid_id" in grid_hour_file.columns
    assert "timestamp" in grid_hour_file.columns
    assert "total_activity" in grid_hour_file.columns

    logger.info(
        "VALIDATION export_summary: PASS"
    )

    # =====================================================
    # ACCEPTANCE CRITERIA
    # =====================================================

    logger.info(
        "========== NP2 ACCEPTANCE CHECK =========="
    )

    # Input rows
    logger.info(
        "INPUT_ROWS=%s",
        processor.input_rows
    )

    # Rows rejected
    logger.info(
        "ROWS_REJECTED=%s",
        processor.rejected_rows
    )

    # Nulls handled
    logger.info(
        "NULLS_HANDLED_BY_CURATED_RULE=%s",
        processor.nulls_handled
    )

    # Output rows
    logger.info(
        "GRID_HOUR_OUTPUT_ROWS=%s",
        len(grid_time_data)
    )

    # Duplicate check
    logger.info(
        "GRID_HOUR_DUPLICATES=%s",
        duplicates
    )

    # Country code check
    country_code_present = (
        "country_code"
        in grid_time_data.columns
    )

    logger.info(
        "COUNTRY_CODE_IN_GRID_HOUR=%s",
        country_code_present
    )

    # -----------------------------------------------------
    # Final acceptance assertions
    # -----------------------------------------------------

    assert duplicates == 0, (
        "Grid/hour duplicate check failed."
    )

    assert len(grid_time_data) < len(time_data), (
        "Grid/hour output must have fewer rows "
        "than its input."
    )

    assert not country_code_present, (
        "country_code must not be present in "
        "grid/hour analytics output."
    )

    assert processor.input_rows >= 0
    assert processor.rejected_rows >= 0
    assert processor.nulls_handled >= 0
    assert len(grid_time_data) > 0

    logger.info(
        "========== NP2 ACCEPTANCE: PASS =========="
    )

    return True


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    # -----------------------------------------------------
    # INPUT CSV
    # -----------------------------------------------------

    file_path = (
        r"C:\Users\semmozhiselvan.a"
        r"\Documents\phase1 proj"
        r"\data\landing"
        r"\sms-call-internet-mi-2013-11-01.csv"
    )

    processor = UsageProcessor(
        file_path=file_path
    )

    # =====================================================
    # 1. LOAD
    # =====================================================

    loaded_data = processor.load_data()

    print("\n===== LOAD VALIDATION =====")

    print(
        "Loaded rows:",
        len(loaded_data)
    )

    print(
        "Loaded columns:",
        len(loaded_data.columns)
    )

    # =====================================================
    # 2. CLEAN
    # =====================================================

    cleaned_data = (
        processor.clean_data()
    )

    print("\n===== CLEAN VALIDATION =====")

    print(
        "Cleaned rows:",
        len(cleaned_data)
    )

    print(
        "Null activity values after cleaning:",
        cleaned_data[
            [
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "internet"
            ]
        ].isna().sum().sum()
    )

    # =====================================================
    # 3. TIME FEATURES
    # =====================================================

    time_data = (
        processor.derive_time_features()
    )

    print(
        "\n===== TIME FEATURE VALIDATION ====="
    )

    print(
        "Distinct dates:",
        time_data["date"].nunique()
    )

    print(
        "Distinct hours:",
        time_data["hour"].nunique()
    )

    # =====================================================
    # 4. GRID/HOUR AGGREGATION
    # =====================================================

    grid_time_data = (
        processor.aggregate_to_grid_time()
    )

    print(
        "\n===== GRID/HOUR VALIDATION ====="
    )

    print(
        "Input rows:",
        len(time_data)
    )

    print(
        "Output rows:",
        len(grid_time_data)
    )

    print(
        "Has country_code:",
        "country_code"
        in grid_time_data.columns
    )

    print(
        "Duplicate grid/hour records:",
        grid_time_data.duplicated(
            subset=[
                "grid_id",
                "timestamp"
            ]
        ).sum()
    )

    # =====================================================
    # 5. ACTIVITY FEATURES
    # =====================================================

    activity_data = (
        processor.derive_activity_features()
    )

    print(
        "\n===== ACTIVITY FEATURES ====="
    )

    print(
        activity_data[
            [
                "timestamp",
                "grid_id",
                "total_sms",
                "total_calls",
                "total_activity"
            ]
        ].head()
    )

    # =====================================================
    # 6. COMPUTE KPIs
    # =====================================================

    kpis = (
        processor.compute_kpis()
    )

    print(
        "\n===== DAILY SUMMARY ====="
    )

    print(
        kpis["daily_summary"]
    )

    print(
        "\n===== GRID SUMMARY ====="
    )

    print(
        kpis["grid_summary"].head()
    )

    # =====================================================
    # 7. EXPORT SUMMARY
    # =====================================================

    output_paths = processor.export_summary(
        output_dir="."
    )

    print(
        "\n===== EXPORTED FILES ====="
    )

    print(
        "Daily summary:",
        output_paths[
            "daily_summary_path"
        ]
    )

    print(
        "Grid summary:",
        output_paths[
            "grid_summary_path"
        ]
    )

    print(
        "Grid/hour analytics:",
        output_paths[
            "grid_hour_analytics_path"
        ]
    )

    # =====================================================
    # ACCEPTANCE TESTS
    # =====================================================

    try:

        run_acceptance_tests(

            processor=processor,

            loaded_data=loaded_data,

            cleaned_data=cleaned_data,

            time_data=time_data,

            grid_time_data=grid_time_data,

            activity_data=activity_data,

            kpis=kpis,

            output_paths=output_paths

        )

        print(
            "\n===== NP2 ACCEPTANCE: PASS ====="
        )

        print(
            "All seven method validations passed."
        )

        print(
            "All acceptance criteria passed."
        )

        print(
            f"Execution log: "
            f"{os.path.abspath(LOG_FILE)}"
        )

    except AssertionError as error:

        logger.error(
            "NP2 ACCEPTANCE: FAIL - %s",
            error
        )

        print(
            "\n===== NP2 ACCEPTANCE: FAIL ====="
        )

        print(
            "Reason:",
            error
        )

        raise

    except Exception as error:

        logger.exception(
            "NP2 execution failed: %s",
            error
        )

        print(
            "\n===== NP2 EXECUTION FAILED ====="
        )

        print(
            "Reason:",
            error
        )

        raise