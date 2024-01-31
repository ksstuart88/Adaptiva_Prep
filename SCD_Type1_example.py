import duckdb

conn = duckdb.connect('attdb.db')

# 1. Update StageDB using SCD type 1 (modified_on > created_on, update rows)
update_query_modify_records = """
SELECT *
FROM raw_Transactions t JOIN stg_Transactions s
ON t.transaction_id = s.transaction_id
WHERE t.updated_on > s.updated_on
"""
results = conn.execute(update_query_modify_records)

"""
duckdb Community has emphasized that duckdb has a tight primary ke constraint, restricting you from updating records 
based on primary key. It follows a strict delete then update policy on the column you specify. If you say
SET pk1 = pk2, then essentially, pk1 is deleted, so duckdb no longer knows what you set accordingly.
The community has said that they understand this and it frustrates the OLAP Data engineering community, and will not
fix this. The suggested fix is to manually delete and manually insert to simulate how the update would normally work

UPDATE stg_Transactions AS
SET 
    stg_Transactions.transaction_id = raw_Transactions.transaction_id
FROM stg_Transactions JOIN raw_Transactions
ON stg_Transactions.transaction_id = raw_Transactions.transaction_id
WHERE raw_Transactions.updated_on > stg_Transactions.updated_on

OR 

UPDATE stg_Transactions AS
SET
    stg_Transactions.transaction_id = (SELECT raw_Transaction_id 
                                        FROM raw_Transactions 
                                        WHERE transaction_id = stg_Transactions_transaction_id )
"""
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

    delete_row = f"DELETE FROM stg_Transactions WHERE transaction_id = {transaction_id}"
    conn.execute(delete_row)
    update_query = f"INSERT INTO stg_Transactions VALUES('{transaction_id}','{employee_id}','{store_id}','{customer_id}','{product_id}','{quantity}','{total}','{currency}','{created_on}','{updated_on}')"
    conn.execute(update_query)

sql_query = """
SELECT * FROM stg_Transactions
ORDER BY transaction_id
"""

results = conn.execute(sql_query)

print("The Updated Stage Data Store using SCD Type 1(overwrite existing records based off slow change (modified "
      "records are greater than original created records) are:")
for row in results.fetchall():
    print(row)
print('\n')

conn.close()

