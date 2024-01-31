"""
Let's assume that the AT&T analytics department resides in the US (Headquarters located in Dallas, TX).
To properly evaluate all metrics, all metrics must be converted to USD

Currency Conversions:
1 USD = 1 USD
1 USD = 0.79 GBP
1 USD = 1.34 CAD
"""
import duckdb
from decimal import *

conn = duckdb.connect('attdb.db', read_only=False)
currency_conversions = {"USD": 1, "GBP": 0.79, "CAD": 1.34}

# 1. Insert Raw Store data into Stage data store
sql_query = """
SELECT * FROM raw_Stores
"""

results = conn.execute(sql_query)

for row in results.fetchall():
    store_id = row[0]
    location = row[1]
    region = row[2]

    insert_stores_query = f"INSERT INTO stg_Stores VALUES('{store_id}','{location}','{region}')"
    conn.execute(insert_stores_query)

sql_query = """
SELECT * FROM stg_Stores
ORDER BY store_id
"""

results = conn.execute(sql_query)

print('Stage Stores results are:')
for row in results.fetchall():
    print(row)
print('\n')

# 2. Insert Raw Employee Data into Stage data storage
sql_query = """
SELECT * FROM raw_Employees
"""

results = conn.execute(sql_query)

for row in results.fetchall():
    employee_id = row[0]
    store_id = row[1]
    first_name = row[2]
    last_name = row[3]
    department = row[4]
    start_date = row[5]
    salary = row[6]
    currency = row[7]
    isActive = row[8]

    converted_salary = salary / Decimal(currency_conversions[currency])

    insert_employee_query = f"INSERT INTO stg_Employees VALUES('{employee_id}','{store_id}','{first_name}','{last_name}','{department}','{start_date}','{converted_salary}','USD','{isActive}')"

    conn.execute(insert_employee_query)

sql_query = """
SELECT * FROM stg_Employees
ORDER BY employee_id
"""

result = conn.execute(sql_query)

print('Stage Employees Results are:')
for row in result.fetchall():
    print(row)
print('\n')

# 3 Insert Raw Products Data into Stage data storage
sql_query = """
SELECT * FROM raw_Products
"""

results = conn.execute(sql_query)

for row in results.fetchall():
    product_id = row[0]
    name = row[1]
    price = row[2]
    currency = row[3]
    description = row[4]
    converted_price = price / Decimal(currency_conversions[currency])

    insert_products_query = f"INSERT INTO stg_Products VALUES('{product_id}','{name}','{converted_price}','USD','{description}')"

    conn.execute(insert_products_query)

sql_query = """
SELECT * FROM stg_Products
ORDER BY product_id
"""

results = conn.execute(sql_query)

print("Stage Products Results are:")
for row in results.fetchall():
    print(row)
print('\n')

# 4 Insert Raw Customers Data into Stage data storage
sql_query = """
SELECT * FROM raw_Customers
"""

results = conn.execute(sql_query)

for row in results.fetchall():
    customer_id = row[0]
    first_name = row[1]
    last_name = row[2]
    email = row[3]

    insert_customers_query = f"INSERT INTO stg_Customers VALUES('{customer_id}','{first_name}','{last_name}','{email}')"

    conn.execute(insert_customers_query)

sql_query = """
SELECT * FROM stg_Customers
ORDER BY customer_id
"""

results = conn.execute(sql_query)

print("Stage Customers results are:")
for row in results.fetchall():
    print(row)
print('\n')

# 5. Insert Raw Transactions Data into Stage data storage
sql_query = """
SELECT * FROM raw_Transactions
"""

results = conn.execute(sql_query)

for row in results.fetchall():
    transaction_id = row[0]
    employee_id = row[1]
    store_id = row[2]
    customer_id = row[3]
    product_id = row[4]
    quantity = row[5]
    total = row[6]
    currency = row[7]
    created_on = row[8]
    updated_on = row[9]

    converted_total = total / Decimal(currency_conversions[currency])

    insert_transactions_query = f"INSERT INTO stg_Transactions VALUES('{transaction_id}','{employee_id}','{store_id}','{customer_id}','{product_id}','{quantity}','{converted_total}','USD','{created_on}','{updated_on}')"

    conn.execute(insert_transactions_query)

sql_query = """
SELECT * FROM stg_Transactions
ORDER BY transaction_id
"""

results = conn.execute(sql_query)

print("Stage Transactions results are:")
for row in results.fetchall():
    print(row)
print('\n')

conn.close()

