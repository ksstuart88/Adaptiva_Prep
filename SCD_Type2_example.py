import duckdb
from decimal import *

# 1. Establish connection to DuckDB
conn = duckdb.connect('attDB.db', read_only=False)

employee_transfer = [(81, 3, 'Tristan', 'Thompson', 'sales', '2023-01-15 11:30:00.123456', Decimal(23.49), 'USD', 'Y')]

# update flag to old employee record in raw employee table
update_raw_query = """
UPDATE raw_Employees
SET isActive = 'N'
WHERE first_name = 'Tristan' AND last_name = 'Thompson'
"""
conn.execute(update_raw_query)

# update flag to old employee record in stage employee table
update_stg_query = """
UPDATE stg_Employees
SET isActive = 'N'
WHERE first_name = 'Tristan' AND last_name = 'Thompson'
"""
conn.execute(update_stg_query)

for employee in employee_transfer:
    employee_id = employee[0]
    store_id = employee[1]
    first_name = employee[2]
    last_name = employee[3]
    department = employee[4]
    start_date = employee[5]
    salary = employee[6]
    currency = employee[7]
    isActive = employee[8]

    # insert new record into raw employee table
    insert_raw_query = f"INSERT INTO raw_Employees VALUES('{employee_id}','{store_id}','{first_name}','{last_name}'," \
                   f"'{department}','{start_date}','{salary}','{currency}','{isActive}')"

    conn.execute(insert_raw_query)

    # insert new record into stage employee table
    insert_stg_query = f"INSERT INTO stg_Employees VALUES('{employee_id}','{store_id}','{first_name}','{last_name}'," \
                   f"'{department}','{start_date}','{salary}','{currency}','{isActive}')"

    conn.execute(insert_stg_query)

# Print results after SCD Type2 update to Raw Employees Table
sql_query = """
SELECT * FROM raw_Employees
ORDER BY employee_id
"""
results = conn.execute(sql_query)

print("Current Raw Employees are:")
for row in results.fetchall():
    print(row)
print('\n')

# Print results after SCD Type2 update to Stage Employees Table
sql_query = """
SELECT * FROM stg_Employees
ORDER BY employee_id
"""
results = conn.execute(sql_query)

print("Current Stage Employees are:")
for row in results.fetchall():
    print(row)
print('\n')

# Ad-hoc query, just to prove that Tristan Thompson should now have 2 records
sql_query = """
SELECT COUNT(*)
FROM raw_Employees
GROUP BY first_name,last_name
HAVING first_name = 'Tristan' AND last_name = 'Thompson'
"""

results = conn.execute(sql_query)

for row in results.fetchall():
    print("Number of entries for Tristan Thompson employee are: " + str(row[0]))

conn.close()



