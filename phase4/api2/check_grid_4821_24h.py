import sqlite3

db = r"C:\Users\semmozhiselvan.a\Documents\phase1 proj\phase3\de6\warehouse\network_analytics.db"

connection = sqlite3.connect(db)

rows = connection.execute("""
SELECT
    dt.timestamp,
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
       WHERE grid_id = 4821
   )
WHERE dt.timestamp BETWEEN '2013-11-06 20:00:00'
                        AND '2013-11-07 19:00:00'
ORDER BY dt.timestamp
""").fetchall()

print("SQL 24-HOUR ROW COUNT:", len(rows))

for row in rows:
    print(row)

connection.close()
