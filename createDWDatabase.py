import duckdb

# 1. Establish connection to DuckDB
conn = duckdb.connect('attdb.db', read_only=False)

""""******************
CREATE RAW TABLES
*******************"""
# 2. Create Raw Stores Table
stores_query = """
CREATE OR REPLACE TABLE raw_Stores (
    store_id INT NOT NULL PRIMARY KEY,
    location VARCHAR(255) NOT NULL,
    region VARCHAR(255) NOT NULL
)
"""
conn.execute(stores_query)

# 3. Create Raw Employees Table
employees_query = """
CREATE OR REPLACE TABLE raw_Employees (
    employee_id INT NOT NULL PRIMARY KEY,
    store_id INT NOT NULL REFERENCES Stores(store_id),
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    department VArCHAR(255) NOT NULL,
    start_date DATETIME NOT NULL,
    salary DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    isActive VARCHAR(1) NOT NULL
)
"""
conn.execute(employees_query)

# 4. Create Raw Products Table
products_query = """
CREATE OR REPLACE TABLE raw_Products (
    product_id INT NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    description VARCHAR(255) 
)
"""
conn.execute(products_query)

# 5. Create Raw Customers Table
customers_query = """
CREATE OR REPLACE TABLE raw_Customers (
    customer_id INT NOT NULL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL
)
"""
conn.execute(customers_query)

# 6. Create Raw Transactions Table
transactions_query = """
CREATE OR REPLACE TABLE raw_Transactions (
    transaction_id INT NOT NULL PRIMARY KEY,
    employee_id INT NOT NULL REFERENCES Employees(employee_id),
    store_id INT NOT NULL References Stores(store_id),
    customer_id INT NOT NULL REFERENCES Customers(customer_id),
    product_id INT NOT NULL REFERENCES Products(product_id),
    quantity INT NOT NULL,
    total DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    created_on DATETIME NOT NULL,
    updated_on DATETIME NOT NULL
)
"""
conn.execute(transactions_query)

""""******************
CREATE STAGE TABLES
*******************"""
# 7. Create Stage Stores Table
stores_query = """
CREATE OR REPLACE TABLE stg_Stores (
    store_id INT NOT NULL PRIMARY KEY,
    location VARCHAR(255) NOT NULL,
    region VARCHAR(255) NOT NULL
)
"""
conn.execute(stores_query)

# 8. Create Stage Employees Table
employees_query = """
CREATE OR REPLACE TABLE stg_Employees (
    employee_id INT NOT NULL PRIMARY KEY,
    store_id INT NOT NULL REFERENCES stg_Stores(store_id),
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    department VArCHAR(255) NOT NULL,
    start_date DATETIME NOT NULL,
    salary DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    isActive VARCHAR(1) NOT NULL
)
"""
conn.execute(employees_query)

# 9. Create Stage Products Table
products_query = """
CREATE OR REPLACE TABLE stg_Products (
    product_id INT NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    description VARCHAR(255) 
)
"""
conn.execute(products_query)

# 10. Create Stage Customers Table
customers_query = """
CREATE OR REPLACE TABLE stg_Customers (
    customer_id INT NOT NULL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL
)
"""
conn.execute(customers_query)

# 11. Create Stage Transactions Table
transactions_query = """
CREATE OR REPLACE TABLE stg_Transactions (
    transaction_id INT NOT NULL PRIMARY KEY,
    employee_id INT NOT NULL REFERENCES stg_Employees(employee_id),
    store_id INT NOT NULL References stg_Stores(store_id),
    customer_id INT NOT NULL REFERENCES stg_Customers(customer_id),
    product_id INT NOT NULL REFERENCES stg_Products(product_id),
    quantity INT NOT NULL,
    total DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    created_on DATETIME NOT NULL,
    updated_on DATETIME NOT NULL
)
"""
conn.execute(transactions_query)

conn.close()
