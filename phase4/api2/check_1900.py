import sqlite3

db = r"C:\Users\semmozhiselvan.a\Documents\phase1 proj\phase3\de6\warehouse\network_analytics.db"

connection = sqlite3.connect(db)

rows = connection.execute("""
SELECT
    dg.grid_id,
    dt.timestamp,
    f.total_activity
FROM fact_network_activity f
JOIN dim_time dt
    ON f.time_key = dt.time_key
JOIN dim_grid dg
    ON f.grid_key = dg.grid_key
WHERE dt.timestamp = '2013-11-07 19:00:00'
ORDER BY dg.grid_id
LIMIT 10
""").fetchall()

print("19:00 ROW COUNT SAMPLE:", len(rows))

for row in rows:
    print(row)

connection.close()
