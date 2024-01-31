import duckdb

conn = duckdb.connect('attdb.db', read_only=True)

"""
agg_sales_by_id:    one employee_id can have multiple transactions. Aggregate stg_transactions based on all sales 
                    by employee_id
                    
agg_sales_by_name:  If employees transfer locations (i.e. Tristan Thompson), they can also have many employee_ids.
                    Aggregate agg_sales_by_id based on only the first and last name to get all the sales from
                    every employee over every location
"""

sql_query = """
WITH agg_sales_by_id AS(
    SELECT 
        e.employee_id AS employee_id,
        e.first_name AS first_name,
        e.last_name AS last_name,
        SUM(t.total) AS total_sales
    FROM stg_Employees e JOIN stg_Transactions t
    ON e.employee_id = t.employee_id
    GROUP BY e.employee_id,e.first_name,e.last_name
), agg_sales_by_name AS (
    SELECT
        first_name AS first_name,
        last_name AS last_name,
        SUM(total_sales) AS total_aggregated_sales
    FROM agg_sales_by_id  
    GROUP BY first_name, last_name   
)

CREATE OR REPLACE TABLE top10earners AS(
    SELECT * FROM agg_sales_by_name
    ORDER BY total_aggregated_sales DESC
    LIMIT 10
)
"""

results = conn.execute(sql_query)

for row in results.fetchall():
    print(row)
print('\n')

conn.close()

