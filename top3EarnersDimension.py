import duckdb

conn = duckdb.connect('attdb.db', read_only=False)

sql_query = """
WITH cte AS(
    SELECT 
        s.location AS location,
        e.first_name AS first_name,
        e.last_name AS last_name,
        e.department AS department,
        e.salary AS salary,
        e.currency AS currency,
        DENSE_RANK() OVER(PARTITION BY e.department ORDER BY e.salary DESC) AS rank
    FROM stg_Employees e JOIN stg_Stores s
    ON e.store_id = s.store_id
)

CREATE OR REPLACE TABLE top3earners AS(
    SELECT * FROM cte
    WHERE rank <= 3
    ORDER BY department
)
"""
results = conn.execute(sql_query)

for row in results.fetchall():
    print(row)
print('\n')
conn.close()

