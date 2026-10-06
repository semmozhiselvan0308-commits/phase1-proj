import sqlite3
from datetime import datetime
from pathlib import Path


class NetworkService:
    def __init__(self, warehouse_path: Path):
        self.warehouse_path = Path(warehouse_path)

    def _connect(self):
        if not self.warehouse_path.exists():
            raise FileNotFoundError(
                f"Warehouse database not found: {self.warehouse_path}"
            )

        return sqlite3.connect(self.warehouse_path)

    def get_summary(self, as_of: datetime | None = None):
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

            summary = connection.execute(
                """
                WITH filtered AS (
                    SELECT
                        dt.timestamp,
                        dg.grid_id,
                        f.total_activity
                    FROM fact_network_activity f
                    JOIN dim_time dt
                        ON f.time_key = dt.time_key
                    JOIN dim_grid dg
                        ON f.grid_key = dg.grid_key
                    WHERE dt.timestamp <= ?
                ),

                activity_totals AS (
                    SELECT
                        COALESCE(SUM(total_activity), 0) AS total_activity,
                        COUNT(DISTINCT grid_id) AS active_grids
                    FROM filtered
                ),

                hourly_totals AS (
                    SELECT
                        timestamp,
                        SUM(total_activity) AS activity
                    FROM filtered
                    GROUP BY timestamp
                ),

                peak AS (
                    SELECT
                        timestamp AS peak_hour
                    FROM hourly_totals
                    ORDER BY activity DESC, timestamp ASC
                    LIMIT 1
                ),

                grid_totals AS (
                    SELECT
                        grid_id,
                        SUM(total_activity) AS activity
                    FROM filtered
                    GROUP BY grid_id
                ),

                top_grid_result AS (
                    SELECT
                        grid_id
                    FROM grid_totals
                    ORDER BY activity DESC, grid_id ASC
                    LIMIT 1
                )

                SELECT
                    activity_totals.total_activity,
                    activity_totals.active_grids,
                    peak.peak_hour,
                    top_grid_result.grid_id
                FROM activity_totals
                CROSS JOIN peak
                CROSS JOIN top_grid_result
                """,
                (effective_as_of.strftime("%Y-%m-%d %H:%M:%S"),),
            ).fetchone()

            if summary is None:
                raise RuntimeError(
                    f"No analytics data available for as_of={effective_as_of}"
                )

            return {
                "total_activity": float(summary[0]),
                "active_grids": int(summary[1]),
                "peak_hour": datetime.fromisoformat(summary[2]),
                "top_grid": int(summary[3]),
                "as_of": effective_as_of,
            }

        finally:
            connection.close()