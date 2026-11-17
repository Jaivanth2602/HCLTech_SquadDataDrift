# Retail Performance Analytics Pipeline

## Project overview

This project turns four retail CSV files into cleaned, queryable KPI datasets and Power BI visuals. It uses an AWS S3 data lake with a Bronze → Silver → Gold medallion flow, AWS Glue for Spark transformations, Amazon Athena for SQL access, and Power BI for visualization.

The pipeline is event-driven at the batch level: after all four CSVs are uploaded, uploading a `_READY` marker starts the AWS workflow. Step Functions runs the Bronze-to-Silver job and then the Silver-to-Gold job in sequence.

## Architecture

```mermaid
flowchart LR
    U[User uploads 4 CSV files] --> B[S3 Bronze<br/>datadrift-bronze-bucket/landing/]
    B --> R[Upload _READY marker]
    R --> E[Amazon EventBridge rule]
    E --> SF[AWS Step Functions<br/>statemachine-pipeline]
    SF --> G1[AWS Glue job<br/>bronze-to-silver]
    G1 --> S[S3 Silver<br/>datadrift-silver-bucket/silver/]
    S --> G2[AWS Glue job<br/>silver-to-gold]
    G2 --> GD[S3 Gold<br/>datadrift-gold-bucket/gold/]
    GD --> C[Glue Crawler<br/>Glue Data Catalog]
    C --> A[Amazon Athena]
    A --> P[Power BI Desktop]
```

## Data sources

The Bronze landing prefix contains these CSVs:

| File | Contents |
|---|---|
| `customers.csv` | Customer identity, contact, demographic, region, and signup date |
| `products.csv` | Product/SKU, category, supplier, store, selling price, and cost price |
| `sales_returns.csv` | Orders, customer/product references, quantity, sales, order/payment modes, and returns |
| `card_payments_refunds.csv` | Card payment/refund transactions, amounts, dates, and status |

## Medallion layers

### Bronze — raw landing files

- **Bucket:** `datadrift-bronze-bucket`
- **Prefix:** `landing/`
- Stores the original CSV files. These files are the source snapshot used for each batch.
- After uploading all four files, upload an empty or small file named `_READY` into `landing/` to indicate that the batch is complete.

### Silver — cleaned and conformed data

- **Bucket:** `datadrift-silver-bucket`
- **Prefix:** `silver/`
- The `bronze-to-silver` AWS Glue PySpark job reads the CSVs, normalizes text and columns, casts numeric/date fields, checks required values, removes duplicate business keys where defined, and validates customer/product/order references.
- Valid records are written as Parquet. Invalid rows and duplicate business keys are written separately to `silver/rejected/` with a rejection reason and source dataset.

Silver datasets:

```text
s3://datadrift-silver-bucket/silver/customers/
s3://datadrift-silver-bucket/silver/products/
s3://datadrift-silver-bucket/silver/sales_returns/
s3://datadrift-silver-bucket/silver/card_payments_refunds/
s3://datadrift-silver-bucket/silver/rejected/
```

### Gold — KPI-ready outputs

- **Bucket:** `datadrift-gold-bucket`
- **Prefix:** `gold/`
- The `silver-to-gold` Glue job reads Silver Parquet data, calculates the KPI aggregates, and writes one Parquet dataset per KPI.

Gold datasets:

```text
s3://datadrift-gold-bucket/gold/top5_products/
s3://datadrift-gold-bucket/gold/top5_customers/
s3://datadrift-gold-bucket/gold/regional_customer_growth/
s3://datadrift-gold-bucket/gold/product_profit_margin/
s3://datadrift-gold-bucket/gold/digital_payment_percentage/
s3://datadrift-gold-bucket/gold/quarterly_sales_growth/
```

## Automated pipeline execution

1. Upload the four complete CSV files to `s3://datadrift-bronze-bucket/landing/`.
2. Upload `s3://datadrift-bronze-bucket/landing/_READY` after all four CSVs are present.
3. S3 sends an `Object Created` event to Amazon EventBridge. The rule matches the exact object key `landing/_READY` and targets the Step Functions state machine `statemachine-pipeline`.
4. Step Functions starts the Glue job `bronze-to-silver` and waits for it to succeed.
5. After that succeeds, Step Functions starts `silver-to-gold` and waits for it to finish.
6. The Gold Parquet datasets are overwritten with the current snapshot results.
7. The Glue Crawler has already registered the Gold folder structure and schemas in the Glue Data Catalog. Athena reads the catalog tables and the Parquet files in S3.
8. Power BI uses the configured Athena connection to load Gold tables and build visuals.

The `_READY` marker prevents the workflow from starting after only one of the four files has arrived. Uploading the marker again creates another event and starts another workflow run.

## KPI outputs

1. **Top five products:** product sales, return quantity/amount, net quantity, and net sales; ranked by net quantity sold.
2. **Top five customers:** distinct orders, gross purchase amount, returns, and net purchase amount; ranked by net purchase amount.
3. **Regional customer growth:** new customer signups aggregated by region and calendar quarter, with quarter-over-quarter growth.
4. **Product sales and profit margin:** gross sales, returns, net sales, estimated cost, profit, and margin percentage.
5. **Digital payment percentage:** payment amounts grouped by `payment_mode`, with cash and cash-on-delivery treated as non-digital.
6. **Quarterly sales growth:** quarterly order counts, gross sales, returns, net sales, previous-quarter net sales, and QoQ growth percentage.

## Athena and Glue Data Catalog

The Glue Crawler was run against `s3://datadrift-gold-bucket/gold/` and created tables in the selected Glue Data Catalog database. It infers the Parquet schemas and records table locations; it does not transform data. Athena uses those table definitions to query Gold. The crawler generally only needs to be rerun if the Gold schema or folder layout changes.

Athena query results are written to a configured S3 query-results location. This is separate from the Gold KPI output folders.

## Power BI

Power BI Desktop connects to Athena through the configured Athena ODBC DSN and loads Gold tables. The initial model was loaded in **Import** mode.

In Import mode, Power BI contains a snapshot. The AWS pipeline updating Gold does not itself refresh the open Power BI Desktop report. Refresh in Desktop manually, or publish the report and configure Power BI Service refresh. Automated refresh from each completed AWS run is a follow-on integration: publish to Power BI Service, configure the Athena ODBC gateway and credentials, and then trigger the Power BI semantic-model refresh after the Step Functions workflow succeeds. The gateway machine must remain available for service refresh.

## Security and audit

Configured access is role-based:

- The `bronze-to-silver` Glue execution role reads Bronze and writes Silver.
- The `silver-to-gold` Glue execution role reads Silver and writes Gold.
- The Step Functions execution role can start and monitor the two named Glue jobs. Its trust relationship allows `states.amazonaws.com` to assume it.
- The Power BI IAM identity is separate from the AWS root user and should only receive the Athena, Glue Catalog, Gold-read, and Athena query-results permissions it needs.

Useful operational audit points:

- Step Functions execution history records which workflow steps ran and their success/failure.
- Glue job run history and CloudWatch Logs contain job status and processing output.
- `silver/rejected/` preserves rows rejected by data quality and relationship checks, with reasons.
- CloudTrail can provide API-level auditing. For object-level S3 access auditing, enable relevant S3 data events and protect the trail destination. These CloudTrail settings are recommended operational additions and should be verified in the account.

## Scalability and operational considerations

- The current jobs process the complete four-file snapshot and overwrite Silver and Gold on each run. This is straightforward and repeatable for full-file batches, but it is not incremental processing.
- For increasing data volume, convert Silver/Gold to incremental processing with stable record keys, batch/run IDs, and partitioned Parquet outputs. Compact small files and partition around common Athena filters such as date or quarter.
- Avoid concurrent runs for the same snapshot; duplicate or overlapping `_READY` uploads may launch multiple workflows. Add a batch identifier/idempotency control before allowing concurrent batch uploads.
- Monitor Step Functions failures, Glue job errors/duration, rejected-row counts, Athena bytes scanned, and Power BI refresh status.
- Set S3 encryption, block public access, lifecycle/retention, and least-privilege bucket policies for each data layer.

## Current status

- Bronze-to-Silver Glue job: working.
- Silver-to-Gold Glue job: working; Gold Parquet outputs created.
- Glue Crawler: run successfully and Gold tables are queryable in Athena.
- EventBridge and Step Functions: configured; `_READY` upload triggers the pipeline.
- Power BI Desktop: connected to Athena and loaded Gold data.
