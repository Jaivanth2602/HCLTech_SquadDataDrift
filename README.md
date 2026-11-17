# HCLTech Squad Data Drift

An AWS serverless data lakehouse for transforming retail data into curated KPI
datasets. The project follows a Bronze-Silver-Gold pattern using Amazon S3 and
AWS Glue, with Amazon Athena used to query the Gold data. Microsoft Power BI is
the planned visualization layer.

## Architecture

```text
Raw CSV data
    |
    v
Amazon S3 - Bronze bucket
    |
    | AWS Glue: bronze_to_silver.py
    v
Amazon S3 - Silver bucket (clean Parquet)
    |
    | AWS Glue: silver_to_gold.py
    v
Amazon S3 - Gold bucket (KPI Parquet datasets)
    |
    | AWS Glue Crawler and Data Catalog
    v
Amazon Athena - SQL queries
    |
    | Planned connection
    v
Microsoft Power BI dashboards
```

The layers are stored in separate S3 buckets:

- **Bronze:** raw source files, preserved in their original CSV structure.
- **Silver:** validated and cleaned Parquet datasets.
- **Gold:** aggregated Parquet datasets designed for KPI analysis.

## Source data

The Bronze bucket contains the following files under the configured prefix:

- `customers.csv`
- `products.csv`
- `sales_returns.csv`
- `card_payments_refunds.csv`

The current Glue jobs use the following example bucket names:

- `datadrift-bronze-bucket`
- `datadrift-silver-bucket`
- `datadrift-gold-bucket`

Replace these names with the bucket names in your AWS account when configuring
the Glue jobs.

## ETL jobs

### 1. Bronze to Silver

[`bronze_to_silver.py`](bronze_to_silver.py) is an AWS Glue PySpark job. It:

- Reads the four CSV sources from the Bronze S3 bucket.
- Normalizes column names and trims string values.
- Parses supported date formats.
- Casts numeric and date fields to appropriate Spark types.
- Rejects invalid records with a `rejection_reason`.
- Removes duplicate customer, product, and payment business keys.
- Validates customer and product foreign-key relationships.
- Writes clean Parquet datasets to the Silver bucket.
- Writes rejected records to the Silver `rejected/` path.

Silver outputs:

```text
s3://<silver-bucket>/<silver-prefix>/customers/
s3://<silver-bucket>/<silver-prefix>/products/
s3://<silver-bucket>/<silver-prefix>/sales_returns/
s3://<silver-bucket>/<silver-prefix>/card_payments_refunds/
s3://<silver-bucket>/<silver-prefix>/rejected/
```

### 2. Silver to Gold

[`silver_to_gold.py`](silver_to_gold.py) reads the Silver Parquet datasets,
joins sales with customer and product attributes, derives net sales and net
quantity, and writes the following Gold KPI datasets:

| Dataset | Purpose |
| --- | --- |
| `top5_products` | Top five products by net quantity sold, including sales and returns |
| `top5_customers` | Top five customers by net purchase amount |
| `regional_customer_growth` | Quarterly new-customer counts and regional QoQ growth |
| `product_profit_margin` | Gross sales, returns, estimated cost, profit, and margin by product |
| `digital_payment_percentage` | Digital payment amount and percentage by payment mode |
| `quarterly_sales_growth` | Quarterly orders, net sales, and QoQ sales growth |

Gold outputs are written as Parquet under:

```text
s3://<gold-bucket>/<gold-prefix>/<dataset>/
```

Both jobs overwrite their snapshot output on each run.

## Running the Glue jobs

Create two Glue Spark jobs and upload the corresponding Python script. Add the
following job parameters.

### Bronze-to-Silver job

```text
--JOB_NAME              <glue-job-name>
--BRONZE_BUCKET         <bronze-bucket>
--BRONZE_PREFIX         landing
--SILVER_BUCKET         <silver-bucket>
--SILVER_PREFIX         silver
```

The Bronze files should therefore be available at:

```text
s3://<bronze-bucket>/landing/customers.csv
s3://<bronze-bucket>/landing/products.csv
s3://<bronze-bucket>/landing/sales_returns.csv
s3://<bronze-bucket>/landing/card_payments_refunds.csv
```

### Silver-to-Gold job

```text
--JOB_NAME              <glue-job-name>
--SILVER_BUCKET         <silver-bucket>
--SILVER_PREFIX         silver
--GOLD_BUCKET           <gold-bucket>
--GOLD_PREFIX           gold
```

Run the Bronze-to-Silver job first, followed by the Silver-to-Gold job.

The Glue job role needs permission to read and write the relevant S3 buckets
and to create and run Glue job resources. Restrict the IAM policy to the
project buckets in production.

## Glue Crawler and Athena

After the Gold job completes:

1. Open **AWS Glue** in the AWS Console.
2. Create a database for the Gold layer, for example `datadrift_gold`.
3. Create a Glue crawler whose data source is the Gold prefix:
   `s3://<gold-bucket>/gold/`.
4. Configure the crawler to use the Gold database and run it.
5. Confirm that the six Gold datasets are available as tables in the Glue Data
   Catalog.
6. Open **Amazon Athena**, select the Gold database, and query the tables.

Example Athena queries:

```sql
SELECT *
FROM datadrift_gold.top5_products
ORDER BY rank;

SELECT *
FROM datadrift_gold.quarterly_sales_growth
ORDER BY year, quarter;

SELECT *
FROM datadrift_gold.product_profit_margin
ORDER BY profit_margin_percentage DESC;
```

Configure an Athena query-result location in S3 before running queries. The
Athena workgroup and the Glue crawler role must have access to the relevant
S3 locations.

## Planned Power BI integration

The next step is to connect Microsoft Power BI to the Gold tables through the
Amazon Athena connector:

1. Ensure the Athena ODBC driver or Power BI Athena connector is installed.
2. Configure the AWS region, Athena workgroup, and query-result S3 location.
3. Connect to the Gold Glue Catalog database.
4. Import the KPI tables or use DirectQuery where appropriate.
5. Build visuals for product performance, customer growth, profit margin,
   digital payments, and quarterly sales growth.

Power BI connectivity and dashboards are planned; they are not part of the
current ETL scripts.

## Repository files

```text
HCLTech_SquadDataDrift/
├── bronze_to_silver.py   # Bronze CSV validation and Silver Parquet job
├── silver_to_gold.py     # Gold KPI aggregation job
└── README.md              # Architecture and deployment guide
```
