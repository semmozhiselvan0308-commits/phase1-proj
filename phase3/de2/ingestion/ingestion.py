
import csv
import shutil
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

LANDING_DIR = BASE_DIR / "data" / "landing"
RAW_DIR = BASE_DIR / "data" / "raw"
REJECTED_DIR = BASE_DIR / "data" / "rejected"
LOG_DIR = BASE_DIR / "logs"

AUDIT_FILE = LOG_DIR / "ingestion_audit.csv"

FILE_PATTERN = "sms-call-internet-mi-*.csv"

REQUIRED_COLUMNS = {
    "timestamp",
    "grid_id",
    "country_code",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
}


# ---------------------------------------------------------
# Setup directories
# ---------------------------------------------------------

def setup_directories():
    """Create required directories if they do not exist."""

    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------
# 1. Detect files
# ---------------------------------------------------------

def detect_files():
    """
    Detect daily Milan CSV files in the landing directory.

    Only files matching:
        sms-call-internet-mi-*.csv

    are considered.
    """

    setup_directories()

    files = sorted(LANDING_DIR.glob(FILE_PATTERN))

    print(f"Detected {len(files)} candidate file(s).")

    for file in files:
        print(f"  - {file.name}")

    return files


# ---------------------------------------------------------
# 2. Validate schema
# ---------------------------------------------------------

def validate_schema(file_path):
    """
    Validate that all required columns exist.

    Unexpected extra columns generate a warning
    but do not reject the file.
    """

    with open(
        file_path,
        "r",
        encoding="utf-8",
        newline=""
    ) as file:

        reader = csv.reader(file)

        try:
            header = next(reader)

        except StopIteration:
            return False, "Empty file: missing header"

    columns = {
        column.strip()
        for column in header
    }

    # -----------------------------------------------------
    # Check required columns
    # -----------------------------------------------------

    missing_columns = REQUIRED_COLUMNS - columns

    if missing_columns:

        missing = ", ".join(
            sorted(missing_columns)
        )

        return (
            False,
            f"Missing required column(s): {missing}"
        )

    # -----------------------------------------------------
    # DE8 Control: unexpected extra columns
    # -----------------------------------------------------

    extra_columns = columns - REQUIRED_COLUMNS

    if extra_columns:

        extra = ", ".join(
            sorted(extra_columns)
        )

        print(
            f"WARNING: Unexpected extra column(s): {extra}"
        )

    return True, "Schema validation passed"


# ---------------------------------------------------------
# 3. Validate minimum quality
# ---------------------------------------------------------

def validate_minimum_quality(file_path):
    """
    Perform basic quality checks:

    - file must contain at least one data row
    - timestamp must be valid
    - activity values must not be negative
    - required activity fields must not be missing
    """

    with open(
        file_path,
        "r",
        encoding="utf-8",
        newline=""
    ) as file:

        reader = csv.DictReader(file)

        row_count = 0

        for row_number, row in enumerate(
            reader,
            start=2
        ):

            row_count += 1

            # -------------------------------------------------
            # Validate timestamp
            # -------------------------------------------------

            timestamp = row.get("timestamp")

            if timestamp is None or timestamp.strip() == "":
                return (
                    False,
                    f"Missing value in timestamp at row "
                    f"{row_number}"
                )

            timestamp = timestamp.strip()

            try:

                datetime.strptime(
                    timestamp,
                    "%Y-%m-%d %H:%M:%S"
                )

            except ValueError:

                return (
                    False,
                    f"Invalid timestamp at row "
                    f"{row_number}: {timestamp}"
                )

            # -------------------------------------------------
            # Validate activity values
            # -------------------------------------------------

            activity_columns = [
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "internet_activity",
            ]

            for column in activity_columns:

                value = row.get(column)

                # ---------------------------------------------
                # DE8 Control: partially corrupt row
                # ---------------------------------------------

                if value is None or value.strip() == "":

                    return (
                        False,
                        f"Missing value in {column} at row "
                        f"{row_number}"
                    )

                value = value.strip()

                # ---------------------------------------------
                # Validate numeric value
                # ---------------------------------------------

                try:

                    numeric_value = float(value)

                except ValueError:

                    return (
                        False,
                        f"Invalid numeric value in "
                        f"{column} at row "
                        f"{row_number}: {value}"
                    )

                # ---------------------------------------------
                # DE8 Control: negative activity
                # ---------------------------------------------

                if numeric_value < 0:

                    return (
                        False,
                        f"Negative activity value "
                        f"in {column} at row "
                        f"{row_number}: {value}"
                    )

        # -----------------------------------------------------
        # Reject empty data files
        # -----------------------------------------------------

        if row_count == 0:

            return (
                False,
                "File contains no data rows"
            )

    return True, "Minimum quality validation passed"


# ---------------------------------------------------------
# Count rows
# ---------------------------------------------------------

def count_rows(file_path):
    """Count data rows in a CSV file."""

    with open(
        file_path,
        "r",
        encoding="utf-8",
        newline=""
    ) as file:

        reader = csv.reader(file)

        try:
            next(reader)

        except StopIteration:
            return 0

        return sum(
            1
            for _ in reader
        )


# ---------------------------------------------------------
# Audit logging
# ---------------------------------------------------------

def write_audit_record(
    filename,
    status,
    row_count,
    reason
):
    """
    Write one ingestion audit record.
    """

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    file_exists = AUDIT_FILE.exists()

    with open(
        AUDIT_FILE,
        "a",
        encoding="utf-8",
        newline=""
    ) as file:

        writer = csv.writer(file)

        if not file_exists:

            writer.writerow([
                "filename",
                "status",
                "row_count",
                "reason",
                "processed_at",
            ])

        writer.writerow([
            filename,
            status,
            row_count,
            reason,
            datetime.now().isoformat(
                timespec="seconds"
            ),
        ])


# ---------------------------------------------------------
# 4. Route file
# ---------------------------------------------------------

def route_file(file_path):
    """
    Validate and route one file.

    Valid files   -> data/raw/
    Invalid files -> data/rejected/
    """

    file_path = Path(file_path)

    row_count = count_rows(file_path)

    # -----------------------------------------------------
    # Prevent silent duplicate processing
    # -----------------------------------------------------

    raw_destination = (
        RAW_DIR / file_path.name
    )

    rejected_destination = (
        REJECTED_DIR / file_path.name
    )

    if (
        raw_destination.exists()
        or rejected_destination.exists()
    ):

        reason = (
            "File already processed; "
            "duplicate routing skipped"
        )

        print(
            f"SKIPPED: {file_path.name}"
        )

        print(
            f"Reason: {reason}"
        )

        write_audit_record(
            file_path.name,
            "SKIPPED",
            row_count,
            reason,
        )

        return "SKIPPED"

    # -----------------------------------------------------
    # Schema validation
    # -----------------------------------------------------

    schema_valid, schema_reason = (
        validate_schema(file_path)
    )

    if not schema_valid:

        shutil.copy2(
            file_path,
            rejected_destination
        )

        write_audit_record(
            file_path.name,
            "REJECTED",
            row_count,
            schema_reason,
        )

        print(
            f"REJECTED: {file_path.name}"
        )

        print(
            f"Reason: {schema_reason}"
        )

        return "REJECTED"

    # -----------------------------------------------------
    # Minimum quality validation
    # -----------------------------------------------------

    quality_valid, quality_reason = (
        validate_minimum_quality(file_path)
    )

    if not quality_valid:

        shutil.copy2(
            file_path,
            rejected_destination
        )

        write_audit_record(
            file_path.name,
            "REJECTED",
            row_count,
            quality_reason,
        )

        print(
            f"REJECTED: {file_path.name}"
        )

        print(
            f"Reason: {quality_reason}"
        )

        return "REJECTED"

    # -----------------------------------------------------
    # Valid file
    # -----------------------------------------------------

    shutil.copy2(
        file_path,
        raw_destination
    )

    write_audit_record(
        file_path.name,
        "ACCEPTED",
        row_count,
        "Schema and minimum quality validation passed",
    )

    print(
        f"ACCEPTED: {file_path.name}"
    )

    print(
        f"Routed to: {raw_destination}"
    )

    return "ACCEPTED"


# ---------------------------------------------------------
# Main execution
# ---------------------------------------------------------

def main():

    print("=" * 70)
    print("DE2 - LANDING TO RAW INGESTION")
    print("=" * 70)

    files = detect_files()

    # -----------------------------------------------------
    # Control 1: Missing daily input must fail ingestion
    # -----------------------------------------------------

    if not files:

        print(
            "ERROR: No daily Milan CSV files found."
        )

        raise RuntimeError(
            "Missing daily input file."
        )

    results = []

    for file in files:

        print()

        status = route_file(file)

        results.append(
            (file.name, status)
        )

    # -----------------------------------------------------
    # Control 2: Rejected files must fail ingestion
    # -----------------------------------------------------

    rejected_files = [
        filename
        for filename, status in results
        if status == "REJECTED"
    ]

    if rejected_files:

        print()
        print("=" * 70)
        print("INGESTION FAILED")
        print("=" * 70)

        for filename in rejected_files:

            print(
                f"Rejected file: {filename}"
            )

        raise RuntimeError(
            f"Ingestion failed: "
            f"{len(rejected_files)} file(s) rejected."
        )

    print()
    print("=" * 70)
    print("INGESTION COMPLETE")
    print("=" * 70)

    print(
        f"Audit log: {AUDIT_FILE}"
    )


if __name__ == "__main__":
    main()

