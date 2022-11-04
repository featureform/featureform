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
    name="",
    account_name="testingstoragegen",
    account_key="",
    container_name="databricksdemo",
    root_path="",
)

# Databricks/EMR just has credentials because it is just a means of processing
databricks = ff.DatabricksCredentials(
    # Can either use username/password or host/token. Add checks that only one pair is given
    username="",
    password="",
    host="",
    token=""
)

# Register spark as the pair of a filestore + executor credentials
spark = ff.register_spark(
    executor=databricks,
    filestore=azure_blob
)

redis = ff.register_redis(
    name=f"redis-quickstart",
    host="quickstart-redis",  # The internal dns name for redis
    port=6379,
    description="A Redis deployment we created for the Featureform quickstart"
)

payments = spark.register_parquet_file(
    name="payments",
    variant=f"{VERSION}",
    description="This dataset includes data about the orders payment options.",
    file_path="https://testingstoragegen.blob.core.windows.net/databricksdemo/ecommerce_order_payments_dataset.parquet",
)

orders = spark.register_parquet_file(
    name="orders",
    variant=f"{VERSION}",
    description="This is the core dataset. From each order you might find all other information.",
    file_path="https://testingstoragegen.blob.core.windows.net/databricksdemo/ecommerce_orders_dataset.parquet",
)

customers = spark.register_parquet_file(
    name="customers",
    variant=f"{VERSION}",
    description="This dataset has information about the customer and its location. Use it to identify unique customers in the orders dataset and to find the orders delivery location.",
    file_path="https://testingstoragegen.blob.core.windows.net/databricksdemo/ecommerce_customers_dataset.parquet",
)

reviews = spark.register_parquet_file(
    name="reviews",
    variant=f"{VERSION}",
    description="This dataset includes data about the reviews made by the customers.",
    file_path="https://testingstoragegen.blob.core.windows.net/databricksdemo/ecommerce_order_reviews_dataset.parquet",
)

order_items = spark.register_parquet_file(
    name="order_items",
    variant=f"{VERSION}",
    description="This dataset includes data about the items purchased within each order.",
    file_path="https://testingstoragegen.blob.core.windows.net/databricksdemo/ecommerce_order_items_dataset.parquet",
)

product_category_translation = spark.register_parquet_file(
    name="product_category_translation",
    variant=f"{VERSION}",
    description="Translates the productcategoryname to english.",
    file_path="https://testingstoragegen.blob.core.windows.net/databricksdemo/ecommerce_product_category_name_translation.parquet",
)

products = spark.register_parquet_file(
    name="products",
    variant=f"{VERSION}",
    description="This dataset includes data about the products sold by Olist.",
    file_path="https://testingstoragegen.blob.core.windows.net/databricksdemo/ecommerce_products_dataset.parquet",
)


@spark.sql_transformation(name="total_paid_per_customer_day",
                          variant=f"{VERSION}",
                          description="the total order value transaction amount for a customer per day")
def total_paid_per_customer_day():
    return "select trunc(order_approved_at,'day') as day_date, customer_unique_id, sum(p.payment_value) as total_order_paid " \
           "from {{payments.default}} p " \
           "left join {{orders.default}} o on (o.order_id = p.order_id) " \
           "left join {{customer.default}} c on (o.customer_id = c.customer_id) " \
           "group by 1,2 "

@spark.sql_transformation(name="total_paid_per_day",
                          variant=f"{VERSION}",
                          description="the total order value transaction amount for a user")
def total_paid_per_day():
    return "select trunc(order_approved_at,'day') as day_date, sum(p.payment_value) as total_order_paid " \
           "from {{payments.default}} p " \
           "left join {{orders.default}} o on (o.order_id = p.order_id) " \
           "left join {{customer.default}} c on (o.customer_id = c.customer_id) " \
           "group by 1 "

@spark.sql_transformation(name="reviews_by_order",
                          variant=f"{VERSION}",
                          description="get reviews by order")
def reviews_by_order():
    return "select order_id, review_score, " \
           "concat(review_comment_title, '-',review_comment_message) as review_text " \
           "from {{reviews.default}} r"

@spark.sql_transformation(name="product_item_spend_by_customer_order",
                          variant=f"{VERSION}",
                          description="product items and spend by order")
def product_item_spend_by_customer_order():
    return "select concat(o.order_id,'-',p.product_id) as order_product_id, " \
           "c.customer_unique_id, o.order_id, t.product_category_name_english, i.price " \
           "from {{order_items.default}} i " \
           "left join {{orders.default}} o on (o.order_id = i.order_id) " \
           "left join {{products.default}} p on (p.product_id  = i.product_id) " \
           "left join {{customer.default}} c on (c.customer_id = o.customer_id) " \
           "left join {{product_category_translation.default}} t on (t.product_category_name = p.product_category_name) "

@spark.sql_transformation(name="order_value_by_customer_order",
                          variant=f"{VERSION}",
                          description="order value by customer, order")
def order_value_by_customer_order():
    return "select o.order_id, c.customer_unique_id, o.order_approved_at, p.payment_value " \
           "from {{orders.default}} o " \
           "left join {{customers.default}} c on (c.customer_id = o.customer_id) " \
           "left join {{payments.default}} p on (p.order_id = o.order_id) "

order = ff.register_entity("order")
customer = ff.register_entity("customer")

reviews_by_order.register_resources(
    entity=order,
    entity_column="order_id",
    inference_store=redis,
    features=[
        {"name": "review_text", "variant": f"{VERSION}", "column": "review_text", "type": "str"},
    ],
    labels=[
        {"name": "review_score", "variant": f"{VERSION}", "column": "review_score", "type": "int"},
    ]
)

ff.register_training_set(
    "sentiment_prediction",
    label=("review_score", f"{VERSION}"),
    features=[("review_text", f"{VERSION}")],
)