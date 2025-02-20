import pandas as pd ### pip install pandas if missing
import duckdb ### pip install duckdb if missing


########## First: Review Existing Unstructured Data and Diagram a New Structured Relational Data Model ##########

### Please check the Fech Rewards Coding Exercise RDM - Analytics Engineer.pdf for the RDM picture ###

### Loading JSON files into dataFrame as per RDM ###
# users
users_df = pd.read_json("Data/users.json", lines=True)
# brands
brands_df = pd.read_json("Data/brands.json", lines=True)
# receipts
receipts_df = pd.read_json("Data/receipts.json", lines=True)

## Note: lines=True is to allow it to read it as a JSONL (JSON Lines)

### Normalize nested values ###
# users
users_df['_id'] = pd.json_normalize(users_df['_id'])

users_date_fields = ['createdDate', 'lastLogin']
for field in users_date_fields:
    users_df[field] = pd.json_normalize(users_df[field]) 
    users_df[field] = pd.to_datetime(users_df[field], unit="ms") #Casting it to date time in ms

# brands
brands_df['_id'] = pd.json_normalize(brands_df['_id'])

brands_df["barcode"] = brands_df["barcode"].astype(str).str.strip()

cpg_series = brands_df["cpg"].apply(pd.Series)

brands_df['cpgId'] = pd.json_normalize(cpg_series['$id'])

brands_df['cpgRef'] = cpg_series['$ref']

# Receipts
receipts_df["_id"] = pd.json_normalize(receipts_df["_id"])

receipts_date_fields = ["createDate", "dateScanned", "finishedDate", "modifyDate", "pointsAwardedDate", "purchaseDate"] 

for field in receipts_date_fields:
    receipts_df[field] = pd.json_normalize(receipts_df[field])
    receipts_df[field] = pd.to_datetime(receipts_df[field], unit = 'ms')

# ReceiptItems - New Table for receipt items nested values
ReceiptItem = receipts_df.explode('rewardsReceiptItemList') #grabbing the nested values inside rewardsReceiptItemList for the new model

ReceiptItem_df = ReceiptItem['rewardsReceiptItemList'].apply(pd.Series) #makes the above dictionary a series with indexes and construct the dataframe

ReceiptItem_df["receiptId"] = ReceiptItem["_id"]

ReceiptItem_df["barcode"] = ReceiptItem_df["barcode"].astype(str).str.strip() #casting to string to be able to match it to the barcode unit in model brands

### drop unnecesary columns since they are derived into other columns ###
# users - None

# brands 
brands_df = brands_df.drop(columns=['cpg'])

#receipts
receipts_df = receipts_df.drop(columns=['rewardsReceiptItemList'])


### Renaming columns for ease of read ###
# users
users_df = users_df.rename(columns= {'_id': 'id'})
# brands 
brands_df = brands_df.rename(columns= {'_id': 'id'})
#receipts
receipts_df = receipts_df.rename(columns= {'_id': 'id'})
receipts_df = receipts_df.rename(columns= {'createDate': 'createdDate'})


### Remove duplicates ###
# users
users_df = users_df.drop_duplicates()
# brands 
brands_df = brands_df.drop_duplicates()
#receipts
receipts_df = receipts_df.drop_duplicates()
# ReceiptItems
ReceiptItem_df = ReceiptItem_df.drop_duplicates()


### Printing few records of data frame ###
#users
print("users dataFrame")
print(users_df.head()) 
print() #New Line

# brands 
print("brands dataFrame")
print(brands_df.head()) 
print() #New Line

#receipts
print("receipts dataFrame")
print(receipts_df.head()) 
print() #New Line

# ReceiptItems
print("ReceiptItem dataFrame")
print(ReceiptItem_df.head()) 
print() #New Line



########## Second: Write queries that directly answer predetermined questions from a business stakeholder ##########
### running SQL queries using DuckDB ###
con = duckdb.connect()

### Load data into DuckDB
con.execute("CREATE TABLE users AS SELECT * FROM users_df")
con.execute("CREATE TABLE brands AS SELECT * FROM brands_df")
con.execute("CREATE TABLE receipts AS SELECT * FROM receipts_df")
con.execute("CREATE TABLE receiptItems AS SELECT * FROM ReceiptItem_df")


## What are the top 5 brands by receipts scanned for most recent month?

top_5_brands_query = """
WITH RecentMonth AS (
    SELECT 
        DATE_TRUNC('month' , MAX(r.dateScanned)) AS latest_month 
    FROM 
        receipts r
        JOIN receiptItems ri ON ri.receiptId = r.id
    WHERE 
        ri.brandcode <> 'None'
)

, BrandReceiptCount AS (
    SELECT 
        ri.brandcode AS brand_code
        , COUNT(DISTINCT r.id) AS receipt_count
        , DATE_TRUNC('month' , r.dateScanned) AS Month
    FROM 
        receipts r
        JOIN receiptItems ri ON ri.receiptId = r.id
        
    WHERE 
        ri.brandcode <> 'None'
        AND DATE_TRUNC('month' , r.dateScanned) >= (SELECT latest_month FROM RecentMonth)
    GROUP BY 
        ri.brandcode
        , Month
)
SELECT 
    brand_code
    , receipt_count
    , Month
FROM 
    BrandReceiptCount
ORDER BY 
    receipt_count DESC
LIMIT 5
"""

top_5_brands_result = con.execute(top_5_brands_query).fetchdf()
print("results from top_5_brands_query")
print(top_5_brands_result)
print() ### New Line


## How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?

ranking_top_5_recent_vs_prior_query = """
WITH Months AS (
    SELECT 
        DATE_TRUNC('month' , MAX(r.dateScanned)) AS latest_month 
        , DATE_TRUNC('month', MAX(dateScanned)) - INTERVAL '1 month' AS previous_month
    FROM 
        receipts r
        JOIN receiptItems ri ON ri.receiptId = r.id
    WHERE 
        ri.brandcode <> 'None'
)

, BrandReceiptCountRecent AS (
    SELECT 
        DATE_TRUNC('month', r.dateScanned) AS month
        , ri.brandcode AS brand_code
        , COUNT(DISTINCT r.id) AS receipt_count
    FROM 
        receipts r
        JOIN receiptItems ri ON ri.receiptId = r.id
    WHERE 
        ri.brandcode <> 'None'
        AND DATE_TRUNC('month', r.dateScanned) IN (SELECT latest_month FROM Months) 
    GROUP BY 
        month
        , ri.brandcode
)

, BrandReceiptCountPrevious AS (
    SELECT 
        DATE_TRUNC('month', r.dateScanned) AS month
        , ri.brandcode AS brand_code
        , COUNT(DISTINCT r.id) AS receipt_count
    FROM 
        receipts r
        JOIN receiptItems ri ON ri.receiptId = r.id
    WHERE 
        ri.brandcode <> 'None'
        AND DATE_TRUNC('month', r.dateScanned) IN (SELECT previous_month FROM Months) 
    GROUP BY 
        month
        , ri.brandcode
)

, BrandReceiptCount AS (
    SELECT
        month
        , brand_code
        , receipt_count
    FROM 
        BrandReceiptCountRecent
    UNION ALL 
    SELECT
        month
        , brand_code
        , receipt_count
    FROM 
        BrandReceiptCountPrevious
)

, RankedBrandsRecent AS (
    SELECT 
        month
        , brand_code
        , receipt_count
        , RANK() OVER (PARTITION BY month ORDER BY receipt_count DESC) AS rank
    FROM 
        BrandReceiptCountRecent
    QUALIFY
        rank <= 5
)

, RankedBrandsPrevious AS (
    SELECT 
        month
        , brand_code
        , receipt_count
        , RANK() OVER (PARTITION BY month ORDER BY receipt_count DESC) AS rank
    FROM 
        BrandReceiptCountPrevious
)


SELECT
    r.brand_code
    , r.receipt_count AS recent_receipt_count
    , r.rank AS Recent_rank
    , p.receipt_count AS previous_receipt_count
    , p.rank AS previous_rank
FROM
    RankedBrandsRecent r
    LEFT JOIN RankedBrandsPrevious p ON r.brand_code = p.brand_code
"""

ranking_top_5_recent_vs_prior_result = con.execute(ranking_top_5_recent_vs_prior_query).fetchdf()
print("results from ranking_top_5_recent_vs_prior_query")
print(ranking_top_5_recent_vs_prior_result)
print() ### New Line


## When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?

average_spend_from_receipts_status_query = """
SELECT 
    CASE WHEN rewardsReceiptStatus = 'FINISHED' 
        THEN 'ACCEPTED' 
        ELSE rewardsReceiptStatus 
    END AS rewards_Receipt_Status
    , AVG(totalSpent) AS average_spend
FROM 
    receipts
WHERE 
    rewardsReceiptStatus IN ('FINISHED', 'REJECTED')
GROUP BY
    rewardsReceiptStatus
"""

average_spend_from_receipts_status_result = con.execute(average_spend_from_receipts_status_query).fetchdf()
print("results from average_spend_from_receipts_status_query")
print(average_spend_from_receipts_status_result)
print() ### New Line



## When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?

total_items_purchased_from_receipts_status_query = """
SELECT 
    CASE WHEN r.rewardsReceiptStatus = 'FINISHED' 
        THEN 'ACCEPTED' 
        ELSE r.rewardsReceiptStatus 
    END AS rewards_Receipt_Status
    , SUM(ri.quantityPurchased) AS total_items_purchased
FROM 
    receipts r
    JOIN receiptItems ri ON r.id = ri.receiptId
WHERE 
    r.rewardsReceiptStatus IN ('FINISHED', 'REJECTED')

GROUP BY 
    r.rewardsReceiptStatus
"""

total_items_purchased_from_receipts_status_result = con.execute(total_items_purchased_from_receipts_status_query).fetchdf()
print("results from total_items_purchased_from_receipts_status_query")
print(total_items_purchased_from_receipts_status_result)
print() ### New Line




## Which brand has the most spend among users who were created within the past 6 months?

brand_with_most_spend_users_created_within_6_months_query = """
WITH RecentUsers AS (
    SELECT 
        id 
    FROM 
        users 
    WHERE 
        createdDate >= CAST('2021-02-28 00:00:00.000' AS TIMESTAMP) - INTERVAL '6 months'
),
AcceptedReceipts AS (
    SELECT 
        * 
    FROM 
        receipts 
    WHERE 
        rewardsReceiptStatus = 'FINISHED'
),
UserReceipts AS (
    SELECT 
        r.id
        , r.userId
        , ri.barcode
        , CAST(ri.finalPrice AS DOUBLE) AS finalPrice
    FROM 
        AcceptedReceipts r
        JOIN receiptItems ri ON r.id = ri.receiptId
    WHERE 
        r.userId IN (SELECT id FROM RecentUsers)
),
BrandSpend AS (
    SELECT 
        b.name AS brand_name
        , SUM(ur.finalPrice) AS total_spend
    FROM 
        UserReceipts ur
        JOIN brands b ON ur.barcode = b.barcode
    GROUP BY 
        b.name
)
SELECT 
    brand_name
    , total_spend
FROM 
    BrandSpend
ORDER BY 
    total_spend DESC
LIMIT 1
"""

brand_with_most_spend_users_created_within_6_months_result = con.execute(brand_with_most_spend_users_created_within_6_months_query).fetchdf()
print("results from brand_with_most_spend_users_created_within_6_months_query")
print(brand_with_most_spend_users_created_within_6_months_result)
print() ### New Line




## Which brand has the most transactions among users who were created within the past 6 months?
brand_with_most_transactions_users_created_within_6_months_query = """
WITH RecentUsers AS (
    SELECT 
        id 
    FROM 
        users 
    WHERE 
        createdDate >= CAST('2021-02-28 00:00:00.000' AS TIMESTAMP) - INTERVAL '6 months'
),
AcceptedReceipts AS (
    SELECT 
        * 
    FROM 
        receipts 
    WHERE 
        rewardsReceiptStatus = 'FINISHED'
),
UserReceipts AS (
    SELECT 
        r.id
        , r.userId
        , ri.barcode
    FROM 
        AcceptedReceipts r
        JOIN receiptItems ri ON r.id = ri.receiptId
    WHERE 
        r.userId IN (SELECT id FROM RecentUsers)
),
BrandTransactions AS (
    SELECT 
        b.name AS brand_name
        , COUNT(ur.id) AS total_transactions
    FROM 
        UserReceipts ur
        JOIN brands b ON ur.barcode = b.barcode
    GROUP BY 
        b.name
)
SELECT 
    brand_name
    , total_transactions
FROM 
    BrandTransactions
ORDER BY 
    total_transactions DESC
LIMIT 1
"""

brand_with_most_transactions_users_created_within_6_months_result = con.execute(brand_with_most_transactions_users_created_within_6_months_query).fetchdf()
print("results from brand_with_most_transactions_users_created_within_6_months_query")
print(brand_with_most_transactions_users_created_within_6_months_result)
print() ### New Line







############# Third: Evaluate Data Quality Issues in the Data Provided

# Using the programming language of your choice (SQL, Python, R, Bash, etc...) identify as many data quality issues as you can. 
# We are not expecting a full blown review of all the data provided, but instead want to know how you explore and evaluate data of questionable provenance.

# commit your code and findings to the git repository along with the rest of the exercise.
test_records_in_file_query = """
SELECT
    COUNT(*) AS test_record_count
FROM
    brands
WHERE
    name ILIKE '%test%'
"""
test_records_in_file_query_result = con.execute(test_records_in_file_query).fetchdf()
print("results from test_records_in_file_query")
print(test_records_in_file_query_result)
print() #New Line




missing_values_query = """
SELECT 
    'users' AS table_name
    , 'id' AS column_name
    , COUNT(*) AS missing_count
FROM 
    users 
WHERE 
    id IS NULL
UNION ALL
SELECT 
    'users' AS table_name 
    , 'createdDate' AS column_name 
    , COUNT(*) AS missing_count 
FROM 
    users 
WHERE 
    createdDate IS NULL
UNION ALL
SELECT 
    'brands' AS table_name
    , 'id' AS column_name
    , COUNT(*) AS missing_count
FROM 
    brands 
WHERE 
    id IS NULL
UNION ALL
SELECT 
    'receipts' AS table_name
    , 'id' AS column_name
    , COUNT(*) AS missing_count
FROM 
    receipts 
WHERE 
    id IS NULL
UNION ALL
SELECT 
    'receiptItems' AS table_name
    , 'barcode' AS column_name
    , COUNT(*) AS missing_count
FROM 
    receiptItems 
WHERE 
    barcode IS NULL;
"""

missing_values_query_result = con.execute(missing_values_query).fetchdf()
print("results from missing_values_query")
print(missing_values_query_result)
print() #New Line



duplicate_values_query = """
SELECT 
    'users' AS src
    , id
    , COUNT(*) AS cnt
FROM 
    users 
GROUP BY 
    src
    , id 
HAVING 
    cnt > 1
UNION ALL 
SELECT
    'brands' AS src
    , id
    , COUNT(*) AS cnt
FROM 
    brands 
GROUP BY 
    src
    , id 
HAVING 
    cnt > 1
UNION ALL
SELECT 
    'receipts' AS src
    , id
    , COUNT(*) AS cnt
FROM 
    receipts 
GROUP BY 
    src
    , id  
HAVING 
    cnt > 1
UNION ALL
SELECT 
    'receiptItems' AS src
    , receiptId AS id
    , COUNT(*) AS cnt
FROM 
    receiptItems 
GROUP BY 
    src
    , id  
HAVING 
    cnt > 1
"""

duplicate_values_query_result = con.execute(duplicate_values_query).fetchdf()
print("results from duplicate_values_query")
print(duplicate_values_query_result)
print() #New Line
#### NOTE: if you want to see the duplicates, please go to lines 79-85 and comment the drop_duplicates command


user_receipts_orphaned_records_query = """
SELECT 
    r.id
    , r.userId 
FROM 
    receipts r
    LEFT JOIN users u ON r.userId = u.id
WHERE 
    u.id IS NULL
"""
user_receipts_orphaned_records_query_result = con.execute(user_receipts_orphaned_records_query).fetchdf()
print("results from user_receipts_orphaned_records_query")
print(user_receipts_orphaned_records_query_result)
print() #New Line




receiptItems_no_receipts_records_query = """
SELECT 
    ri.receiptId 
FROM 
    receiptItems ri
    LEFT JOIN receipts r ON ri.receiptId = r.id
WHERE 
    r.id IS NULL;
"""
receiptItems_no_receipts_records_query_result = con.execute(receiptItems_no_receipts_records_query).fetchdf()
print("results from receiptItems_no_receipts_records_query")
print(receiptItems_no_receipts_records_query_result)
print() #New Line




created_or_scanned_before_purchase_records_query = """
SELECT 
    id
    , purchaseDate
    , createdDate
    , dateScanned
FROM 
    receipts
WHERE 
    purchaseDate > createdDate 
    OR purchaseDate > dateScanned;
"""
created_or_scanned_before_purchase_records_query_result = con.execute(created_or_scanned_before_purchase_records_query).fetchdf()
print("results from created_or_scanned_before_purchase_records_query")
print(created_or_scanned_before_purchase_records_query_result)
print() #New Line




created_in_future_records_query = """
SELECT 
    id
    , createdDate 
FROM 
    users 
WHERE 
    createdDate > CURRENT_DATE
"""
created_in_future_records_query_result = con.execute(created_in_future_records_query).fetchdf()
print("results from created_in_future_records_query")
print(created_in_future_records_query_result)
print() #New Line



high_item_count_query = """
SELECT 
    receiptId
    , COUNT(*) AS item_count
FROM 
    receiptItems
GROUP BY 
    receiptId
HAVING 
    item_count > 100; -- Adjust based on typical item behavior
"""
high_item_count_query_result = con.execute(high_item_count_query).fetchdf()
print("results from high_item_count_query")
print(high_item_count_query_result)
print() #New Line





user_high_transactions_query = """
SELECT 
    userId
    , COUNT(*) AS transaction_count
FROM 
    receipts
GROUP BY 
    userId
HAVING 
    transaction_count > 500; -- Adjust based on typical user/receipt behavior
"""
user_high_transactions_query_result = con.execute(user_high_transactions_query).fetchdf()
print("results from user_high_transactions_query")
print(user_high_transactions_query_result)
print() #New Line




brand_receiptItems_inconsistencies_query = """
SELECT 
    DISTINCT ri.barcode
FROM 
    receiptItems ri
    LEFT JOIN brands b ON ri.barcode = b.barcode
WHERE 
    b.barcode IS NULL;
"""
brand_receiptItems_inconsistencies_query_result = con.execute(brand_receiptItems_inconsistencies_query).fetchdf()
print("results from brand_receiptItems_inconsistencies_query")
print(brand_receiptItems_inconsistencies_query_result)
print() #New Line


one_to_many_brand_barcodes_query = """
SELECT 
    barcode
    , COUNT(DISTINCT name) brand_name_count
FROM 
    brands 
GROUP BY 
    barcode 
HAVING 
    brand_name_count > 1;
"""
one_to_many_brand_barcodes_query_result = con.execute(one_to_many_brand_barcodes_query).fetchdf()
print("results from one_to_many_brand_barcodes_query")
print(one_to_many_brand_barcodes_query_result)
print() #New Line

one_to_many_brand_name_query = """
SELECT 
    name
    , COUNT(DISTINCT barcode) AS barcode_count
FROM 
    brands 
GROUP BY 
    name 
HAVING 
    barcode_count > 1;
"""
one_to_many_brand_name_query_result = con.execute(one_to_many_brand_name_query).fetchdf()
print("results from one_to_many_brand_name_query")
print(one_to_many_brand_name_query_result)
print() #New Line


one_to_many_receipt_items_barcodes_query = """
SELECT 
    barcode
    , COUNT(DISTINCT description) AS description_count
FROM 
    receiptItems 
GROUP BY 
    barcode 
HAVING 
    description_count > 1;
"""
one_to_many_receipt_items_barcodes_query_result = con.execute(one_to_many_receipt_items_barcodes_query).fetchdf()
print("results from one_to_many_receipt_items_barcodes_query")
print(one_to_many_receipt_items_barcodes_query_result)
print() #New Line