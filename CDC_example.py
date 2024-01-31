import os
import duckdb

conn = duckdb.connect('attdb.db')

# 1. Read in WAL logs and updated raw DB
wal_filepath = './WAL/'
files = os.listdir(wal_filepath)

for file in files:
    # print(file)
    f = open(wal_filepath + file, "r")
    for log_transaction in f:
        sql_query = log_transaction
        conn.execute(sql_query)

sql_query = """
SELECT * FROM raw_Transactions
ORDER BY transaction_id
"""

results = conn.execute(sql_query)

print("Updated Results to Raw Data Storage After WAL reads are:")
for row in results.fetchall():
    print(row)
print('\n')

# 2. Update StageDB using CDC (rows in raw that are not in stage)
stage_size_query = """
SELECT COUNT(*) FROM stg_Transactions
"""
stage_size_results = conn.execute(stage_size_query)
stage_size = stage_size_results.fetchone()[0]

update_query_new_records = f"SELECT * FROM raw_Transactions WHERE transaction_id > {stage_size}"

results = conn.execute(update_query_new_records)

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

    insert_query = f"INSERT INTO stg_Transactions VALUES ('{transaction_id}','{employee_id}','{store_id}','{customer_id}','{product_id}','{quantity}','{total}','{currency}','{created_on}','{updated_on}')"

    conn.execute(insert_query)

sql_query = """
SELECT * FROM stg_Transactions
ORDER BY transaction_id
"""

results = conn.execute(sql_query)

print("Results to Stage Transctions after CDC Update are:")
for row in results.fetchall():
    print(row)
print('\n')

conn.close()

