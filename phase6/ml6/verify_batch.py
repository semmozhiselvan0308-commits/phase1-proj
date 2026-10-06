import sqlite3

db = sqlite3.connect(
    "phase3/de6/warehouse/network_analytics.db"
)

table_exists = db.execute(
    """
    SELECT 1
    FROM sqlite_master
    WHERE type = 'table'
      AND name = 'network_risk_scores'
    """
).fetchone()

print("TABLE EXISTS:", table_exists is not None)

row_count = db.execute(
    "SELECT COUNT(*) FROM network_risk_scores"
).fetchone()[0]

print("ROW COUNT:", row_count)

unique_keys = db.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT DISTINCT grid_id, feature_timestamp
        FROM network_risk_scores
    )
    """
).fetchone()[0]

print("UNIQUE KEYS:", unique_keys)

versions = db.execute(
    """
    SELECT model_version, COUNT(*)
    FROM network_risk_scores
    GROUP BY model_version
    """
).fetchall()

print("MODEL VERSIONS:", versions)

db.close()
