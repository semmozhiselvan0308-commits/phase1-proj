from pathlib import Path
import csv
import sqlite3
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


# =========================================================
# PATHS
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

WAREHOUSE_DB = (
    PROJECT_ROOT
    / "phase3"
    / "de6"
    / "warehouse"
    / "network_analytics.db"
)

ALERT_FILE = (
    PROJECT_ROOT
    / "phase1"
    / "np3"
    / "network_alerts.csv"
)


# =========================================================
# FASTAPI APP
# =========================================================

app = FastAPI(
    title="API3 - Hotspot & Alert API",
    version="1.0.0",
    description=(
        "Rule-based network hotspot and operational alert API "
        "using the analytics warehouse and NP3 alert output."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5176",
        "http://127.0.0.1:5176",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# RESPONSE MODELS
# =========================================================

class Hotspot(BaseModel):
    grid_id: int
    timestamp: str

    sms_in: float
    sms_out: float
    call_in: float
    call_out: float

    total_sms_activity: float
    total_call_activity: float
    internet_activity: float
    total_activity: float
    internet_share: float

    status: str
    reason: str

    # Future-safe ML fields
    risk_score: Optional[float] = None
    risk_level: Optional[str] = None
    model_version: Optional[str] = None


class Alert(BaseModel):
    grid_id: int
    timestamp: str
    alert_type: str
    severity: str

    current_activity: float
    baseline_activity: float

    total_sms_activity: Optional[float] = None
    total_call_activity: Optional[float] = None
    internet_activity: Optional[float] = None
    total_activity: Optional[float] = None
    internet_share: Optional[float] = None

    reason: str

    # Future-safe ML fields
    risk_score: Optional[float] = None
    risk_level: Optional[str] = None
    model_version: Optional[str] = None


class HotspotResponse(BaseModel):
    items: list[Hotspot]
    count: int
    limit: int
    as_of: Optional[str] = None


class AlertResponse(BaseModel):
    items: list[Alert]
    count: int
    limit: int
    severity: Optional[str] = None
    as_of: Optional[str] = None


# =========================================================
# DATABASE HELPERS
# =========================================================

def get_connection():
    if not WAREHOUSE_DB.exists():
        raise HTTPException(
            status_code=500,
            detail="Analytics warehouse database was not found.",
        )

    connection = sqlite3.connect(WAREHOUSE_DB)
    connection.row_factory = sqlite3.Row
    return connection


def validate_as_of(as_of: Optional[str]) -> Optional[str]:
    if as_of is None:
        return None

    try:
        parsed = datetime.fromisoformat(as_of)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid as_of. Use ISO format such as "
                "2013-11-07T19:00:00."
            ),
        )

    return parsed.strftime("%Y-%m-%d %H:%M:%S")


# =========================================================
# NP3 ALERT HELPERS
# =========================================================

SEVERITY_BY_ALERT_TYPE = {
    "HIGH_ACTIVITY": "high",
    "ACTIVITY_SPIKE": "medium",
    "ACTIVITY_DROP": "medium",
}


def load_np3_alerts():
    if not ALERT_FILE.exists():
        raise HTTPException(
            status_code=500,
            detail="NP3 alert file was not found.",
        )

    with ALERT_FILE.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as file:

        reader = csv.DictReader(file)

        required_columns = {
            "grid_id",
            "timestamp",
            "alert_type",
            "current_activity",
            "baseline_activity",
            "reason",
        }

        if not required_columns.issubset(
            set(reader.fieldnames or [])
        ):
            raise HTTPException(
                status_code=500,
                detail="NP3 alert file has an unexpected schema.",
            )

        alerts = []

        for row in reader:
            alert_type = row["alert_type"]

            severity = SEVERITY_BY_ALERT_TYPE.get(
                alert_type,
                "medium",
            )

            alerts.append(
                {
                    "grid_id": int(row["grid_id"]),
                    "timestamp": row["timestamp"],
                    "alert_type": alert_type,
                    "severity": severity,
                    "current_activity": float(
                        row["current_activity"]
                    ),
                    "baseline_activity": float(
                        row["baseline_activity"]
                    ),
                    "reason": row["reason"],
                }
            )

    return alerts


# =========================================================
# HEALTH CHECK
# =========================================================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "api3",
    }


# =========================================================
# GET /network/hotspots
# =========================================================

@app.get(
    "/network/hotspots",
    response_model=HotspotResponse,
)
def get_hotspots(
    limit: int = Query(
        default=10,
        ge=1,
    ),
    as_of: Optional[str] = Query(
        default=None,
    ),
):

    normalized_as_of = validate_as_of(as_of)

    connection = get_connection()

    try:

        if normalized_as_of is None:

            query = """
                SELECT
                    dg.grid_id,
                    dt.timestamp,
                    f.sms_in,
                    f.sms_out,
                    f.call_in,
                    f.call_out,
                    f.total_sms_activity,
                    f.total_call_activity,
                    f.internet_activity,
                    f.total_activity,
                    f.internet_share
                FROM fact_network_activity f
                JOIN dim_time dt
                    ON f.time_key = dt.time_key
                JOIN dim_grid dg
                    ON f.grid_key = dg.grid_key
                WHERE dt.timestamp = (
                    SELECT MAX(timestamp)
                    FROM dim_time
                )
                ORDER BY
                    f.total_activity DESC,
                    dg.grid_id ASC
                LIMIT ?
            """

            rows = connection.execute(
                query,
                (limit,),
            ).fetchall()

        else:

            query = """
                SELECT
                    dg.grid_id,
                    dt.timestamp,
                    f.sms_in,
                    f.sms_out,
                    f.call_in,
                    f.call_out,
                    f.total_sms_activity,
                    f.total_call_activity,
                    f.internet_activity,
                    f.total_activity,
                    f.internet_share
                FROM fact_network_activity f
                JOIN dim_time dt
                    ON f.time_key = dt.time_key
                JOIN dim_grid dg
                    ON f.grid_key = dg.grid_key
                WHERE dt.timestamp = (
                    SELECT MAX(timestamp)
                    FROM dim_time
                    WHERE timestamp <= ?
                )
                ORDER BY
                    f.total_activity DESC,
                    dg.grid_id ASC
                LIMIT ?
            """

            rows = connection.execute(
                query,
                (
                    normalized_as_of,
                    limit,
                ),
            ).fetchall()

    finally:
        connection.close()

    items = []

    for row in rows:

        items.append(
            Hotspot(
                grid_id=int(row["grid_id"]),
                timestamp=row["timestamp"],

                sms_in=float(row["sms_in"]),
                sms_out=float(row["sms_out"]),
                call_in=float(row["call_in"]),
                call_out=float(row["call_out"]),

                total_sms_activity=float(
                    row["total_sms_activity"]
                ),
                total_call_activity=float(
                    row["total_call_activity"]
                ),
                internet_activity=float(
                    row["internet_activity"]
                ),
                total_activity=float(
                    row["total_activity"]
                ),
                internet_share=float(
                    row["internet_share"]
                ),

                status="hotspot",

                reason=(
                    "High total network activity for "
                    "the selected hourly observation."
                ),
            )
        )

    return HotspotResponse(
        items=items,
        count=len(items),
        limit=limit,
        as_of=as_of,
    )


# =========================================================
# GET /network/alerts
# =========================================================

@app.get(
    "/network/alerts",
    response_model=AlertResponse,
)
def get_alerts(
    limit: int = Query(
        default=10,
        ge=1,
    ),
    severity: Optional[str] = Query(
        default=None,
    ),
    as_of: Optional[str] = Query(
        default=None,
    ),
):

    normalized_as_of = validate_as_of(as_of)

    if severity is not None:
        severity = severity.lower()

        allowed_severities = {
            "high",
            "medium",
        }

        if severity not in allowed_severities:
            raise HTTPException(
                status_code=400,
                detail=(
                    "severity must be one of: "
                    "high, medium."
                ),
            )

    alerts = load_np3_alerts()

    # -----------------------------------------------------
    # Filter by severity
    # -----------------------------------------------------

    if severity is not None:

        alerts = [
            alert
            for alert in alerts
            if alert["severity"] == severity
        ]

    # -----------------------------------------------------
    # Filter by as_of
    # -----------------------------------------------------

    if normalized_as_of is not None:

        alerts = [
            alert
            for alert in alerts
            if alert["timestamp"] <= normalized_as_of
        ]

    # -----------------------------------------------------
    # Deterministic ordering
    # -----------------------------------------------------

    severity_rank = {
        "high": 0,
        "medium": 1,
    }

    alerts.sort(
        key=lambda alert: (
            severity_rank.get(
                alert["severity"],
                99,
            ),
            alert["timestamp"],
            alert["grid_id"],
            alert["alert_type"],
        )
    )

    alerts = alerts[:limit]

    # -----------------------------------------------------
    # Enrich alerts with warehouse measures
    # -----------------------------------------------------

    connection = get_connection()

    try:

        items = []

        for alert in alerts:

            row = connection.execute(
                """
                SELECT
                    f.total_sms_activity,
                    f.total_call_activity,
                    f.internet_activity,
                    f.total_activity,
                    f.internet_share
                FROM fact_network_activity f
                JOIN dim_time dt
                    ON f.time_key = dt.time_key
                JOIN dim_grid dg
                    ON f.grid_key = dg.grid_key
                WHERE dg.grid_id = ?
                  AND dt.timestamp = ?
                LIMIT 1
                """,
                (
                    alert["grid_id"],
                    alert["timestamp"],
                ),
            ).fetchone()

            items.append(
                Alert(
                    grid_id=alert["grid_id"],
                    timestamp=alert["timestamp"],
                    alert_type=alert["alert_type"],
                    severity=alert["severity"],

                    current_activity=alert[
                        "current_activity"
                    ],
                    baseline_activity=alert[
                        "baseline_activity"
                    ],

                    total_sms_activity=(
                        float(row["total_sms_activity"])
                        if row
                        else None
                    ),

                    total_call_activity=(
                        float(row["total_call_activity"])
                        if row
                        else None
                    ),

                    internet_activity=(
                        float(row["internet_activity"])
                        if row
                        else None
                    ),

                    total_activity=(
                        float(row["total_activity"])
                        if row
                        else None
                    ),

                    internet_share=(
                        float(row["internet_share"])
                        if row
                        else None
                    ),

                    reason=alert["reason"],
                )
            )

    finally:
        connection.close()

    return AlertResponse(
        items=items,
        count=len(items),
        limit=limit,
        severity=severity,
        as_of=as_of,
    )