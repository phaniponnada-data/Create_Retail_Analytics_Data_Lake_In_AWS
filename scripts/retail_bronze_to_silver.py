import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "RUN_ID"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = args["BUCKET"]
run_id = args["RUN_ID"]
raw = f"s3://{bucket}/raw/source=retail"
curated = f"s3://{bucket}/curated"
quarantine = f"s3://{bucket}/quarantine"

customer_schema = StructType([
    StructField("customer_id", StringType()), StructField("first_name", StringType()),
    StructField("last_name", StringType()), StructField("email", StringType()),
    StructField("country", StringType()), StructField("created_at", StringType())])
product_schema = StructType([
    StructField("product_id", StringType()), StructField("product_name", StringType()),
    StructField("category", StringType()), StructField("unit_price", DecimalType(12, 2)),
    StructField("active", BooleanType())])
order_schema = StructType([
    StructField("order_id", StringType()), StructField("customer_id", StringType()),
    StructField("order_date", DateType()), StructField("status", StringType()),
    StructField("total_amount", DecimalType(12, 2)), StructField("updated_at", StringType())])
item_schema = StructType([
    StructField("order_id", StringType()), StructField("line_number", IntegerType()),
    StructField("product_id", StringType()), StructField("quantity", IntegerType()),
    StructField("unit_price", DecimalType(12, 2))])
event_schema = StructType([
    StructField("event_id", StringType()), StructField("order_id", StringType()),
    StructField("status", StringType()), StructField("event_ts", StringType())])

def read_csv(entity, schema):
    return (spark.read.option("header", "true").schema(schema)
            .csv(f"{raw}/entity={entity}/*/*.csv")
            .withColumn("source_file", F.input_file_name())
            .withColumn("run_id", F.lit(run_id))
            .withColumn("ingested_at", F.current_timestamp()))

def write_bad(df, entity):
    if not df.rdd.isEmpty():
        (df.write.mode("overwrite").parquet(
            f"{quarantine}/entity={entity}/run_id={run_id}/"))

customers = read_csv("customers", customer_schema)
products = read_csv("products", product_schema)
orders = read_csv("orders", order_schema)
items = read_csv("order_items", item_schema)
events = read_csv("order_status_events", event_schema)

customers = customers.withColumn(
    "reject_reason",
    F.when(F.col("customer_id").isNull(), "NULL_CUSTOMER_ID")
     .when(~F.col("email").contains("@"), "INVALID_EMAIL")
     .when(F.col("country").isNull(), "NULL_COUNTRY"))
valid_customers = customers.filter("reject_reason is null").drop("reject_reason")
write_bad(customers.filter("reject_reason is not null"), "customers")

products = products.withColumn(
    "reject_reason",
    F.when(F.col("product_id").isNull(), "NULL_PRODUCT_ID")
     .when(F.col("unit_price") <= 0, "NON_POSITIVE_PRICE")
     .when(F.col("category").isNull(), "NULL_CATEGORY"))
valid_products = products.filter("reject_reason is null").drop("reject_reason")
write_bad(products.filter("reject_reason is not null"), "products")

order_counts = orders.withColumn("key_count", F.count("*").over(Window.partitionBy("order_id")))
known_customers = valid_customers.select("customer_id").withColumn("customer_exists", F.lit(True))
orders_checked = order_counts.join(known_customers, "customer_id", "left").withColumn(
    "reject_reason",
    F.when(F.col("order_id").isNull(), "NULL_ORDER_ID")
     .when(F.col("key_count") > 1, "DUPLICATE_ORDER_ID")
     .when(F.col("customer_exists").isNull(), "UNKNOWN_CUSTOMER")
     .when(~F.col("status").isin("PLACED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"), "INVALID_STATUS")
     .when(F.col("total_amount") < 0, "NEGATIVE_TOTAL")
     .when(F.col("order_date").isNull(), "INVALID_ORDER_DATE"))
valid_orders = orders_checked.filter("reject_reason is null").drop("key_count", "customer_exists", "reject_reason")
write_bad(orders_checked.filter("reject_reason is not null"), "orders")

known_orders = valid_orders.select("order_id", "order_date").withColumn("order_exists", F.lit(True))
known_products = valid_products.select("product_id").withColumn("product_exists", F.lit(True))
items_checked = (items.join(known_orders, "order_id", "left")
                 .join(known_products, "product_id", "left")
                 .withColumn("reject_reason",
                    F.when(F.col("order_exists").isNull(), "UNKNOWN_OR_INVALID_ORDER")
                     .when(F.col("product_exists").isNull(), "UNKNOWN_PRODUCT")
                     .when(F.col("line_number").isNull(), "NULL_LINE_NUMBER")
                     .when(F.col("quantity") <= 0, "NON_POSITIVE_QUANTITY")
                     .when(F.col("unit_price") <= 0, "NON_POSITIVE_PRICE")))
valid_items = items_checked.filter("reject_reason is null").drop("order_exists", "product_exists", "reject_reason")
write_bad(items_checked.filter("reject_reason is not null"), "order_items")

valid_events = (events.join(valid_orders.select("order_id"), "order_id", "inner")
                .filter(F.col("status").isin("PLACED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED")))

(valid_customers.write.mode("overwrite").option("compression", "snappy").parquet(f"{curated}/customers/"))
(valid_products.write.mode("overwrite").option("compression", "snappy").parquet(f"{curated}/products/"))
(valid_orders.write.mode("overwrite").partitionBy("order_date").option("compression", "snappy").parquet(f"{curated}/orders/"))
(valid_items.write.mode("overwrite").partitionBy("order_date").option("compression", "snappy").parquet(f"{curated}/order_items/"))
(valid_events.write.mode("overwrite").option("compression", "snappy").parquet(f"{curated}/order_status_events/"))

job.commit()
