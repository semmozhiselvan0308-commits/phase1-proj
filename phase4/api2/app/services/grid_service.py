import sqlite3
from datetime import datetime, timedelta
from pathlib import Path


class GridService:
    def __init__(self, warehouse_path: Path):
        self.warehouse_path = Path(warehouse_path)

    def _connect(self):
        if not self.warehouse_path.exists():
            raise FileNotFoundError(
                f"Warehouse database not found: {self.warehouse_path}"
            )

        return sqlite3.connect(self.warehouse_path)

    def grid_exists(self, grid_id: int) -> bool:
        if grid_id < 1 or grid_id > 10000:
            return False

        connection = self._connect()

        try:
            row = connection.execute(
                """
                SELECT 1
                FROM dim_grid
                WHERE grid_id = ?
                LIMIT 1
                """,
                (grid_id,),
            ).fetchone()

            return row is not None

        finally:
            connection.close()

    def get_activity(
        self,
        grid_id: int,
        date: str | None = None,
        hour: int | None = None,
        as_of: datetime | None = None,
    ):
        connection = self._connect()

        try:
            latest_row = connection.execute(
                """
                SELECT MAX(timestamp)
                FROM dim_time
                """
            ).fetchone()

            if not latest_row or latest_row[0] is None:
                raise RuntimeError(
                    "Analytics warehouse contains no timestamps"
                )

            latest_timestamp = datetime.fromisoformat(latest_row[0])
            effective_as_of = as_of or latest_timestamp

            if date is None and hour is None:
                start_time = effective_as_of - timedelta(hours=23)

                rows = connection.execute(
                    """
                    SELECT
                        dt.timestamp,
                        dt.date,
                        dt.hour,
                        COALESCE(f.total_sms_activity, 0),
                        COALESCE(f.total_call_activity, 0),
                        COALESCE(f.internet_activity, 0),
                        COALESCE(f.total_activity, 0)
                    FROM dim_time dt
                    LEFT JOIN fact_network_activity f
                        ON f.time_key = dt.time_key
                       AND f.grid_key = (
                           SELECT grid_key
                           FROM dim_grid
                           WHERE grid_id = ?
                       )
                    WHERE dt.timestamp >= ?
                      AND dt.timestamp <= ?
                    ORDER BY dt.timestamp ASC
                    """,
                    (
                        grid_id,
                        start_time.strftime("%Y-%m-%d %H:%M:%S"),
                        effective_as_of.strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                ).fetchall()

            else:
                params = [grid_id]

                query = """
                    SELECT
                        dt.timestamp,
                        dt.date,
                        dt.hour,
                        COALESCE(f.total_sms_activity, 0),
                        COALESCE(f.total_call_activity, 0),
                        COALESCE(f.internet_activity, 0),
                        COALESCE(f.total_activity, 0)
                    FROM dim_time dt
                    LEFT JOIN fact_network_activity f
                        ON f.time_key = dt.time_key
                       AND f.grid_key = (
                           SELECT grid_key
                           FROM dim_grid
                           WHERE grid_id = ?
                       )
                    WHERE 1 = 1
                """

                if date is not None:
                    query += " AND dt.date = ?"
                    params.append(date)

                if hour is not None:
                    query += " AND dt.hour = ?"
                    params.append(hour)

                query += " AND dt.timestamp <= ?"
                params.append(
                    effective_as_of.strftime("%Y-%m-%d %H:%M:%S")
                )

                query += " ORDER BY dt.timestamp ASC"

                rows = connection.execute(
                    query,
                    params,
                ).fetchall()

            return [
                {
                    "timestamp": datetime.fromisoformat(row[0]),
                    "date": row[1],
                    "hour": int(row[2]),
                    "sms_activity": float(row[3]),
                    "call_activity": float(row[4]),
                    "internet_activity": float(row[5]),
                    "total_activity": float(row[6]),
                }
                for row in rows
            ]

        finally:
            connection.close()