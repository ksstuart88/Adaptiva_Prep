import duckdb

conn = duckdb.connect('attdb.db', read_only=False)

# Create Stores Dimension Table
create_dim_store_query = """
CREATE OR REPLACE TABLE dim_Stores AS (
    SELECT 
        store_id,
        location,
        region
    FROM stg_Stores
)
"""
conn.execute(create_dim_store_query)

# Create Employees Dimension Table
create_dim_employees_query = """
CREATE OR REPLACE TABLE dim_Employees AS (
    SELECT 
        employee_id,
        first_name,
        last_name,
        department,
        start_date,
        currency,
        isActive
    FROM stg_Employees
)
"""
conn.execute(create_dim_employees_query)

# Create Products Dimension Table
create_dim_products_table_query = """
CREATE OR REPLACE TABLE dim_Products AS(
    SELECT
        product_id,
        name,
        currency,
        description
    FROM stg_Products
)
"""
conn.execute(create_dim_products_table_query)

# Create Customers Dimension Table
create_dim_customers_table_query = """
CREATE OR REPLACE TABLE dim_Customers AS(
    SELECT
        customer_id,
        first_name,
        last_name,
        email
    FROM stg_Customers
)
"""
conn.execute(create_dim_customers_table_query)

# Create Transactions Dimension Table
create_dim_transactions_table_query = """
CREATE OR REPLACE TABLE dim_Transactions AS(
    SELECT
        transaction_id,
        currency,
        created_on,
        updated_on
    FROM stg_Transactions
)
"""
conn.execute(create_dim_transactions_table_query)

# Create fact Sales table
create_fact_sales_table_query = """
CREATE OR REPLICATE TABLE fact_Sales AS (
    SELECT
        t.transaction_id,
        t.employee_id,
        t.store_id,
        t.customer_id,
        t.product_id,
        t.quantity,
        t.total      
    FROM stg_Transactions t
)
"""
conn.execute(create_fact_sales_table_query)

conn.close()



