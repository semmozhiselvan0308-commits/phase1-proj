import sqlite3

db = r".\phase3\de6\warehouse\network_analytics.db"

con = sqlite3.connect(db)

print("=" * 60)
print("DE6 WAREHOUSE VALIDATION")
print("=" * 60)

print("\nTABLES:")
tables = con.execute("""
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
    ORDER BY name
""").fetchall()

for table in tables:
    print(table[0])

print("\ndim_time:", con.execute(
    "SELECT COUNT(*) FROM dim_time"
).fetchone()[0])

print("dim_grid:", con.execute(
    "SELECT COUNT(*) FROM dim_grid"
).fetchone()[0])

print("fact_network_activity:", con.execute(
    "SELECT COUNT(*) FROM fact_network_activity"
).fetchone()[0])

print("latest_timestamp:", con.execute(
    "SELECT MAX(timestamp) FROM dim_time"
).fetchone()[0])

print("total_activity:", con.execute(
    "SELECT SUM(total_activity) FROM fact_network_activity"
).fetchone()[0])

con.close()

print("\n" + "=" * 60)
print("VALIDATION COMPLETE")
print("=" * 60)