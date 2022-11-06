import os
from dotenv import load_dotenv

import featureform as ff

FILE_DIRECTORY = os.getenv("FEATUREFORM_TEST_PATH", "")
featureform_location = os.path.dirname(os.path.dirname(FILE_DIRECTORY))
env_file_path = os.path.join(featureform_location, ".env")
load_dotenv(env_file_path)


def get_random_string():
    import random
    import string
    return "".join(random.choice(string.ascii_lowercase) for _ in range(10))


def save_version(version):
    global FILE_DIRECTORY
    with open(f"{FILE_DIRECTORY}/version.txt", "w+") as f:
        f.write(version)


VERSION = get_random_string()
os.environ["TEST_CASE_VERSION"] = VERSION
save_version(VERSION)

# Start of Featureform Definitions
ff.register_user("featureformer").make_default_owner()

# Register blob store because it can be used standalone as an online store as well
azure_blob = ff.register_blob_store(
    name="blob-store",
    account_name="testingstoragegen",
    account_key=os.getenv("AZURE_ACCOUNT_KEY"),
    container_name="databricksdemo",
    root_path=f"databricks_mongo_e2e_{VERSION}",
)

# Databricks/EMR just has credentials because it is just a means of processing
databricks = ff.DatabricksCredentials(
    # Can either use username/password or host/token. Add checks that only one pair is given
    username="",
    password="",
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
    cluster_id=os.getenv("DATABRICKS_CLUSTER")
)

# Register spark as the pair of a filestore + executor credentials
spark = ff.register_spark(
    executor=databricks,
    filestore=azure_blob
)

mongo = ff.register_mongodb(
    name="mongodb-quickstart",
    host=os.getenv("MONGODB_HOST"),
    port=os.getenv("MONGODB_PORT"),
    username=os.getenv("MONGODB_USERNAME"),
    password=os.getenv("MONGODB_PASSWORD"),
    database=os.getenv("MONGODB_DATABASE"),
    throughput=10000
)

payments = spark.register_parquet_file(
    name="payments",
    variant="default",
    description="This dataset includes data about the orders payment options.",
    file_path="https://testingstoragegen.blob.core.windows.net/databricksdemo/ecommerce_order_payments_dataset.parquet",
)

orders = spark.register_parquet_file(
    name="orders",
    variant="default",
    description="This is the core dataset. From each order you might find all other information.",
    file_path="https://testingstoragegen.blob.core.windows.net/databricksdemo/ecommerce_orders_dataset.parquet",
)

customers = spark.register_parquet_file(
    name="customers",
    variant="default",
    description="This dataset has information about the customer and its location. Use it to identify unique customers in the orders dataset and to find the orders delivery location.",
    file_path="https://testingstoragegen.blob.core.windows.net/databricksdemo/ecommerce_customers_dataset.parquet",
)

reviews = spark.register_parquet_file(
    name="reviews",
    variant="default",
    description="This dataset includes data about the reviews made by the customers.",
    file_path="https://testingstoragegen.blob.core.windows.net/databricksdemo/ecommerce_order_reviews_dataset.parquet",
)

order_items = spark.register_parquet_file(
    name="order_items",
    variant="default",
    description="This dataset includes data about the items purchased within each order.",
    file_path="https://testingstoragegen.blob.core.windows.net/databricksdemo/ecommerce_order_items_dataset.parquet",
)

product_category_translation = spark.register_parquet_file(
    name="product_category_translation",
    variant="default",
    description="Translates the productcategoryname to english.",
    file_path="https://testingstoragegen.blob.core.windows.net/databricksdemo/ecommerce_product_category_name_translation.parquet",
)

products = spark.register_parquet_file(
    name="products",
    variant="default",
    description="This dataset includes data about the products sold by Olist.",
    file_path="https://testingstoragegen.blob.core.windows.net/databricksdemo/ecommerce_products_dataset.parquet",
)


@spark.sql_transformation(name="total_paid_per_customer_per_day",
                          variant="default",
                          description="Get the daily total value of payments per customer.")
def total_paid_per_customer_per_day():
    return "select trunc(order_approved_at,'day') as day_date, c.customer_unique_id, sum(p.payment_value) as total_customer_order_paid " \
           "from {{payments.default}} p " \
           "join {{orders.default}} o on (o.order_id = p.order_id) " \
           "join {{customers.default}} c on (c.customer_id = o.customer_id) " \
           "group by 1,2 order by 1 asc "

# Get total payments per day
@spark.sql_transformation(name="total_paid_per_day",
                          variant="default",
                          description="Get the daily total value of payments.")
def total_paid_per_day():
    return "select trunc(order_approved_at,'day') as day_date, sum(p.payment_value) as total_order_paid " \
           "from {{payments.default}} p " \
           "join {{orders.default}} o on (o.order_id = p.order_id) " \
           "group by 1 order by 1 asc "

@spark.df_transformation(inputs=[("total_paid_per_customer_per_day", "default")], variant="default")
def average_daily_transaction(df):
    from pyspark.sql.functions import mean
    df.groupBy("day_date").agg(mean("total_customer_order_paid").alias("average_order_value"))
    return df

@spark.sql_transformation(name="reviews_by_order",
                          variant="default",
                          description="get reviews by order")
def reviews_by_order():
    return "select order_id, review_score, " \
           "concat(review_comment_title, '-',review_comment_message) as review_text " \
           "from {{reviews.default}} r " \
           "where (review_comment_title is not null) and (review_comment_message is not null) " \
           "and (review_comment_title <> '') and (review_comment_message <> '') " \
           "limit 50"

@spark.sql_transformation(name="calculate_month_1_month_6_dates",
                          variant="default",
                          description="Calculate month 1 - 6")
def calculate_month_1_month_6_dates():
    return "select customer_unique_id, to_date(earliest_purchase) as earliest_datetime_purchase, " \
           "date_add(to_date(earliest_purchase),30) as  month_1_datetime, " \
           "date_add(to_date(earliest_purchase),150) as  month_6_datetime " \
           "from (select c.customer_unique_id, min(o.order_approved_at) as earliest_purchase " \
           "from {{orders.default}} o join {{customers.default}} c on (c.customer_id = o.customer_id) group by 1 " \
           "limit 100)"

@spark.sql_transformation(name="get_month_1_customer_spend",
                          variant="default",
                          description="Get month 1 customer spend")
def get_month_1_customer_spend():
    return "select cm.customer_unique_id, sum(p.payment_value) as month_1_value " \
           "from {{orders.default}} o " \
           "join {{customers.default}} c on (c.customer_id = o.customer_id) " \
           "join {{calculate_month_1_month_6_dates.default}} cm on (cm.customer_unique_id = c.customer_unique_id) " \
           "join {{payments.default}} p on (p.order_id = o.order_id) " \
           "where (to_date(o.order_approved_at) < month_1_datetime) or (to_date(o.order_approved_at) = month_1_datetime) " \
           "group by 1"

@spark.sql_transformation(name="get_month_6_customer_spend",
                          variant="default",
                          description="Get month 6 customer spend")
def get_month_6_customer_spend():
    return "select cm.customer_unique_id, sum(p.payment_value) as month_6_value " \
           "from {{orders.default}} o " \
           "join {{customers.default}} c on (c.customer_id = o.customer_id) " \
           "join {{calculate_month_1_month_6_dates.default}} cm on (cm.customer_unique_id = c.customer_unique_id) " \
           "join {{payments.default}} p on (p.order_id = o.order_id) " \
           "where (to_date(o.order_approved_at) < month_6_datetime) and (to_date(o.order_approved_at) > month_1_datetime) " \
           "group by 1"

order = ff.register_entity("order")
customer = ff.register_entity("customer")
daydate = ff.register_entity("daydate")

total_paid_per_day.register_resources(
    entity=daydate,
    entity_column="day_date",
    inference_store=mongo,
    labels=[
        {"name": "total_order_paid", "variant": "default", "column": "total_order_paid", "type": "float32"},
    ]
)

average_daily_transaction.register_resources(
    entity=daydate,
    entity_column="day_date",
    inference_store=mongo,
    features=[
        {"name": "average_order_value", "variant": "default", "column": "average_order_value", "type": "float32"},
    ],
)

reviews_by_order.register_resources(
    entity=order,
    entity_column="order_id",
    inference_store=mongo,
    features=[
        {"name": "review_text", "variant": "default", "column": "review_text", "type": "str"},
    ],
    labels=[
        {"name": "review_score", "variant": "default", "column": "review_score", "type": "int"},
    ]
)

calculate_month_1_month_6_dates.register_resources(
    entity=customer,
    entity_column="customer_unique_id",
    inference_store=mongo,
    features=[
        {"name": "earliest_datetime_purchase", "variant": "default", "column": "earliest_datetime_purchase", "type": "date"},
        {"name": "month_1_datetime", "variant": "default", "column": "month_1_datetime", "type": "date"},
        {"name": "month_6_datetime", "variant": "default", "column": "month_6_datetime", "type": "date"},
    ],
)

get_month_1_customer_spend.register_resources(
    entity=customer,
    entity_column="customer_unique_id",
    inference_store=mongo,
    features=[
        {"name": "month_1_value", "variant": "default", "column": "month_1_value", "type": "float32"},
    ],
)

get_month_6_customer_spend.register_resources(
    entity=customer,
    entity_column="customer_unique_id",
    inference_store=mongo,
    labels=[
        {"name": "month_6_value", "variant": "default", "column": "month_6_value", "type": "float32"},
    ]
)

ff.register_training_set(
    "customerLTV_training",
    label="month_6_value",
    features=["month_1_value"],
)

ff.register_training_set(
    "sentiment_prediction",
    label="review_score",
    features=["review_text"],
)