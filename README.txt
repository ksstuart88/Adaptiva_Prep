*********************************************************************************************************************************************************************************************************
NOTE: duckdb is very much a community/dev lightweight OLAP database. There are many issues the community  has about some of the design decisions making it so tight that you cannot properly execute SQL 
commands you may easily be able to run using an actual DW solution environment

a.updating a row based on primary key equivalent (raw --> stage for updates) is based on a tightly constrained process, which will not execute as normally would (must delete and then insert)
b. creating a table based off of a query does not seem to be as standard SQL command 
	for top3EarnersDimension.py and top10salesearners.py, you need to delete the CREATE OR REPLACE TABLE AS and just run the wuery to get the view
c. Multiple table joins in standard format do not seem to execute in duckdb. The code is more for discussion about the design.
*********************************************************************************************************************************************************************************************************

*********************************************************************************************************************************************************************************************************
General pipeline
createDWDatabase.py | insertRawData.py | insertStageData.py | CDC_example.py | SCD_Type1_example.py | SCD_Type2_example.py | top3earnersDimension.py(remove CREATE and just run query) | 
top10salesearnersDimension.py(remove CREATE and just run query) | create_fact_dim_table.py (for discussion)
*********************************************************************************************************************************************************************************************************

*********************************************************************************************************************************************************************************************************
INITIAL SET UP

1. run createDWDatabase.py to create a centralized data store to house data from various production systems
2. run insertRawData.py to extract production data from external sources and integrate into a centralize data warehouse storage
	total is not a field in the transactions data. This is because a lookup was done to get the price of the item using the product id.
	the total was multiplied by the price * quantity. In real world, this is how total is calculated, so no need to manually force that as a value
3. run createStageDatabase.py to create the Stage data storage 
4. run insertStageData.py to transfer data from raw data storage after applying basic currency conversion transformations
	a dictionary was created to quickly map the currency conversion rates
*********************************************************************************************************************************************************************************************************

*********************************************************************************************************************************************************************************************************
DW MAINTAINENCE

SIMULATE A CHANGE DATA CAPTURE USING WRITE AHEAD LOG (WAL)
5. Create a Write Ahead Log (WAL) Directory that contains new
	transactions (INSERTS) and updated transactions(UPDATE, assume
	that for the Atlanta stores, each customer triples their
	quantities)
6. read the WAL file in and update the raw database
7. use the raw data database to update the stage database as follows
	a. write new transaction entries for all new records
	b. update all existing records where updated_on > created_on

SIMULATE SLOWLY CHANGING DATA (SCD) Type 1 and Type 2
SCD Type 1 Example (update stage columns based on modified date)
8. CREATE A pseudo transaction lot that records the following
	add new sales transactions to atlanta stores
	assume each customer from atlanta transactions came back and wanted to triple their quantities. (update updated_on) 
9. Update the raw data storage based off of this transaction log
10. Update the stage data storage accordingly using SCD type 1 (update columns based on modified date,a)

SCD Type 2 Example(save history and add new row)
11. Suppose Tristan Thompson from the Atlanta store has had so much sales success that he is asked to transfer to a much higher traffic LA market.
	add a new row that sets his location to the LA market, setting the 'current' flag to true for LA and the 'current' flag for the Atlanta market
	as 'false'
*********************************************************************************************************************************************************************************************************

*********************************************************************************************************************************************************************************************************
QUERIES
12. Create a 'top3earners' model that returns a result of the top 3 earners from each department (b)
13. Create a 'top10salesearners' model that returns the result of the top 10 sales earners at AT&T. In real world, you could query this and then assign a 3% bonus to those employees (b)
*********************************************************************************************************************************************************************************************************

*********************************************************************************************************************************************************************************************************
FACT
14. Create a fact_Transaction table using denormalized approach (c)
15. Create Dimension Tables (c)
*********************************************************************************************************************************************************************************************************