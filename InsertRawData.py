import pandas as pd
import duckdb
import os

# 1. Connect to database
conn = duckdb.connect('attdb.db', read_only=False)

# 2. Insert Initial Store Data into Raw data storage
stores_filepath = './Stores_Data/'
files = os.listdir(stores_filepath)
frames = []

for file in files:
    curr_df = pd.read_csv(stores_filepath + file)
    frames.append(curr_df)
stores = pd.concat(frames)

for i in range(len(stores)):
    store_id = stores.iloc[i]['store_id']
    location = stores.iloc[i]['location']
    region = stores.iloc[i]['region']

    insert_stores_query = f"INSERT INTO raw_Stores VALUES('{store_id}','{location}','{region}')"
    conn.execute(insert_stores_query)

sql_query = """
SELECT * FROM raw_Stores
ORDER BY store_id
"""

results = conn.execute(sql_query)

print('Raw Stores results are:')
for row in results.fetchall():
    print(row)
print('\n')

# 3. Insert Initial raw Employee Data into Raw data storage
employees_filepath = './Employees_Data/'

files = os.listdir(employees_filepath)
frames = []

for file in files:
    curr_df = pd.read_csv(employees_filepath + file)
    frames.append(curr_df)
employees = pd.concat(frames)

for i in range(len(employees)):
    employee_id = employees.iloc[i]['employee_id']
    store_id = employees.iloc[i]['store_id']
    first_name = employees.iloc[i]['first_name']
    last_name = employees.iloc[i]['last_name']
    department = employees.iloc[i]['department']
    start_date = employees.iloc[i]['start_date']
    salary = employees.iloc[i]['salary']
    currency = employees.iloc[i]['currency']
    isActive = employees.iloc[i]['isActive']

    insert_employee_query = f"INSERT INTO raw_Employees VALUES ('{employee_id}','{store_id}','{first_name}','{last_name}','{department}','{start_date}','{salary}','{currency}','{isActive}') "

    conn.execute(insert_employee_query)

sql_query = """
SELECT * FROM raw_Employees
ORDER BY employee_id
"""

result = conn.execute(sql_query)

print('Raw Employees Results are:')
for row in result.fetchall():
    print(row)
print('\n')

# 3 Insert Initial raw Products Data into Raw data storage
products_filepath = './Products_Data/'
files = os.listdir(products_filepath)
frames = []

for file in files:
    curr_df = pd.read_csv(products_filepath + file)
    frames.append(curr_df)
products = pd.concat(frames)

for i in range(len(products)):
    product_id = products.iloc[i]['product_id']
    name = products.iloc[i]['name']
    price = products.iloc[i]['price']
    currency = products.iloc[i]['currency']
    description = products.iloc[i]['description']

    insert_products_query = f"INSERT INTO raw_Products VALUES('{product_id}','{name}','{price}','{currency}','{description}')"

    conn.execute(insert_products_query)

sql_query = """
SELECT * FROM raw_Products
ORDER BY product_id
"""

results = conn.execute(sql_query)

print("Raw Products Results are:")
for row in results.fetchall():
    print(row)
print('\n')

# 4 Insert Initial raw Customers Data into Raw data storage
customers_filepath = './Customers_Data/'
files = os.listdir(customers_filepath)
frames = []

for file in files:
    curr_df = pd.read_csv(customers_filepath + file)
    frames.append(curr_df)
customers = pd.concat(frames)

for i in range(len(customers)):
    customer_id = customers.iloc[i]['customer_id']
    first_name = customers.iloc[i]['first_name']
    last_name = customers.iloc[i]['last_name']
    email = customers.iloc[i]['email']

    insert_customers_query = f"INSERT INTO raw_Customers VALUES('{customer_id}','{first_name}','{last_name}','{email}')"

    conn.execute(insert_customers_query)

sql_query = """
SELECT * FROM raw_Customers
ORDER BY customer_id
"""

results = conn.execute(sql_query)

print("Raw Customers results are:")
for row in results.fetchall():
    print(row)
print('\n')

# 5 Insert Initial raw Transactions Data into Raw data storage
transactions_filepath = './Transactions_Data/'
files = os.listdir(transactions_filepath)
frames = []

for file in files:
    curr_df = pd.read_csv(transactions_filepath + file)
    frames.append(curr_df)
transactions = pd.concat(frames)

for i in range(len(transactions)):
    transaction_id = transactions.iloc[i]['transaction_id']
    employee_id = transactions.iloc[i]['employee_id']
    store_id = transactions.iloc[i]['store_id']
    customer_id = transactions.iloc[i]['customer_id']
    product_id = transactions.iloc[i]['product_id']
    quantity = transactions.iloc[i]['quantity']

    price_query = f"SELECT price FROM raw_Products WHERE product_id = {product_id}"
    price_result = conn.execute(price_query)
    price = price_result.fetchone()
    total = price[0] * quantity

    currency = transactions.iloc[i]['currency']
    created_on = transactions.iloc[i]['created_on']
    updated_on = transactions.iloc[i]['updated_on']

    insert_transactions_query = f"INSERT INTO raw_Transactions VALUES('{transaction_id}','{employee_id}','{store_id}','{customer_id}','{product_id}','{quantity}','{total}','{currency}','{created_on}','{updated_on}')"

    conn.execute(insert_transactions_query)

sql_query = """
SELECT * FROM raw_Transactions
ORDER BY transaction_id
"""

results = conn.execute(sql_query)

print("Raw Transactions results are:")
for row in results.fetchall():
    print(row)
print('\n')

conn.close()

