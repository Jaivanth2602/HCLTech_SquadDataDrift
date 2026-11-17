 # AWS Glue PySpark job: Silver -> Gold KPI datasets.
#
# Glue job name: silver-to-gold
# Required job parameters (Job details > Advanced properties > Job parameters):
#   --SILVER_BUCKET  datadrift-silver-bucket
#   --SILVER_PREFIX  silver
#   --GOLD_BUCKET    datadrift-gold-bucket
#   --GOLD_PREFIX    gold
#
# Reads the outputs of retail_bronze_to_silver_complete_glue.py and writes
# Parquet KPI datasets to s3://datadrift-gold-bucket/gold/<dataset>/.
# Each run overwrites the Gold datasets.

import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SILVER_BUCKET", "SILVER_PREFIX", "GOLD_BUCKET", "GOLD_PREFIX"],
)

silver_root = f"s3://{args['SILVER_BUCKET']}/{args['SILVER_PREFIX'].strip('/')}/"
gold_root = f"s3://{args['GOLD_BUCKET']}/{args['GOLD_PREFIX'].strip('/')}/"

sc = SparkContext.getOrCreate()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)


def read_silver(dataset):
    return spark.read.parquet(silver_root + dataset + "/")


customers = read_silver("customers")
products = read_silver("products")
sales = read_silver("sales_returns")
payments = read_silver("card_payments_refunds")

# Attach customer/product attributes and derive net values used by KPIs.
sales_details = (
    sales
    .join(products, "product_id", "left")
    .join(customers, "customer_id", "left")
    .withColumn(
        "net_quantity_sold",
        F.coalesce(F.col("quantity"), F.lit(0))
        - F.coalesce(F.col("return_quantity"), F.lit(0)),
    )
    .withColumn(
        "net_sales",
        F.coalesce(F.col("sales_amount"), F.lit(0))
        - F.coalesce(F.col("return_amount"), F.lit(0)),
    )
)


# KPI 1: top five products by net quantity, with order/return measures.
product_totals = (
    sales_details
    .groupBy("product_id", "product_name", "category")
    .agg(
        F.sum("quantity").alias("total_quantity_sold"),
        F.sum("sales_amount").alias("total_sales"),
        F.sum("return_quantity").alias("total_return_quantity"),
        F.sum("return_amount").alias("total_return_amount"),
        F.sum("net_quantity_sold").alias("net_quantity_sold"),
        F.sum("net_sales").alias("net_sales"),
    )
)
product_rank = Window.orderBy(
    F.desc("net_quantity_sold"), F.desc("net_sales"), F.asc("product_id")
)
top5_products = (
    product_totals.withColumn("rank", F.row_number().over(product_rank))
    .filter(F.col("rank") <= 5)
)


# KPI 1: top five customers by net purchase amount, with order/return measures.
customer_totals = (
    sales_details
    .groupBy("customer_id", "customer_name", "region")
    .agg(
        F.countDistinct("sales_order_id").alias("total_orders"),
        F.sum("sales_amount").alias("total_purchase_amount"),
        F.sum("return_amount").alias("total_returns_amount"),
        F.sum("net_sales").alias("net_purchase_amount"),
    )
)
customer_rank = Window.orderBy(
    F.desc("net_purchase_amount"), F.desc("total_orders"), F.asc("customer_id")
)
top5_customers = (
    customer_totals.withColumn("rank", F.row_number().over(customer_rank))
    .filter(F.col("rank") <= 5)
)


# KPI 2: quarterly new customer signups and their QoQ growth by region.
regional_quarterly = (
    customers.filter(F.col("signup_date").isNotNull())
    .withColumn("year", F.year("signup_date"))
    .withColumn("quarter", F.quarter("signup_date"))
    .groupBy("region", "year", "quarter")
    .agg(F.countDistinct("customer_id").alias("new_customers"))
)
region_window = Window.partitionBy("region").orderBy("year", "quarter")
regional_customer_growth = (
    regional_quarterly
    .withColumn("previous_quarter_customers", F.lag("new_customers").over(region_window))
    .withColumn(
        "customer_growth_percentage",
        F.when(
            F.col("previous_quarter_customers") > 0,
            F.round(
                (F.col("new_customers") - F.col("previous_quarter_customers"))
                * 100 / F.col("previous_quarter_customers"),
                2,
            ),
        ).otherwise(F.lit(None).cast("double")),
    )
)


# KPI 3: product sales versus margin. Cost is estimated using net units sold
# multiplied by the product's cost_price.
product_margin = (
    sales_details
    .groupBy("product_id", "product_name", "category")
    .agg(
        F.sum("sales_amount").alias("gross_sales"),
        F.sum("return_amount").alias("returns_amount"),
        F.sum("net_sales").alias("net_sales"),
        F.sum(
            F.coalesce(F.col("net_quantity_sold"), F.lit(0))
            * F.coalesce(F.col("cost_price"), F.lit(0))
        ).alias("estimated_cost"),
    )
    .withColumn("profit", F.col("net_sales") - F.col("estimated_cost"))
    .withColumn(
        "profit_margin_percentage",
        F.when(
            F.col("net_sales") != 0,
            F.round(F.col("profit") * 100 / F.col("net_sales"), 2),
        ).otherwise(F.lit(None).cast("double")),
    )
)


# KPI 4: digital payment share by sales amount. Cash and COD are non-digital.
digital_modes = [
    "credit card", "debit card", "upi", "online", "net banking", "wallet"
]
payment_totals = (
    sales
    .withColumn("payment_mode_normalized", F.lower(F.trim("payment_mode")))
    .groupBy("payment_mode_normalized")
    .agg(F.sum("sales_amount").alias("payment_amount"))
    .withColumn("is_digital", F.col("payment_mode_normalized").isin(digital_modes))
)
all_payments = Window.partitionBy()
digital_payment_percentage = (
    payment_totals
    .withColumn("total_payment_amount", F.sum("payment_amount").over(all_payments))
    .withColumn(
        "total_digital_payment_amount",
        F.sum(F.when(F.col("is_digital"), F.col("payment_amount")).otherwise(0))
        .over(all_payments),
    )
    .withColumn(
        "digital_payment_percentage",
        F.when(
            F.col("total_payment_amount") > 0,
            F.round(
                F.col("total_digital_payment_amount") * 100
                / F.col("total_payment_amount"),
                2,
            ),
        ).otherwise(F.lit(None).cast("double")),
    )
)


# KPI 5: quarterly sales and QoQ sales growth.
quarterly_sales = (
    sales_details
    .filter(F.col("order_date").isNotNull())
    .withColumn("year", F.year("order_date"))
    .withColumn("quarter", F.quarter("order_date"))
    .groupBy("year", "quarter")
    .agg(
        F.countDistinct("sales_order_id").alias("total_orders"),
        F.sum("sales_amount").alias("gross_sales"),
        F.sum("return_amount").alias("returns_amount"),
        F.sum("net_sales").alias("net_sales"),
    )
)
quarter_window = Window.orderBy("year", "quarter")
quarterly_sales_growth = (
    quarterly_sales
    .withColumn("previous_quarter_sales", F.lag("net_sales").over(quarter_window))
    .withColumn(
        "sales_growth_percentage",
        F.when(
            F.col("previous_quarter_sales") > 0,
            F.round(
                (F.col("net_sales") - F.col("previous_quarter_sales"))
                * 100 / F.col("previous_quarter_sales"),
                2,
            ),
        ).otherwise(F.lit(None).cast("double")),
    )
)


gold_tables = {
    "top5_products": top5_products,
    "top5_customers": top5_customers,
    "regional_customer_growth": regional_customer_growth,
    "product_profit_margin": product_margin,
    "digital_payment_percentage": digital_payment_percentage,
    "quarterly_sales_growth": quarterly_sales_growth,
}

for table_name, dataframe in gold_tables.items():
    output_path = gold_root + table_name + "/"
    dataframe.write.mode("overwrite").parquet(output_path)
    print(f"Wrote {table_name} to {output_path}")

job.commit()
