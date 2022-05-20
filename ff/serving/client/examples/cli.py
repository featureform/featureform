# Run in command line with:
# python3 -m featureform examples/cli.py -host <hostname:port>
from featureform import register as ff

user = ff.register_user("test")
user.make_default_owner()
snowflake = ff.register_snowflake(
    name="snowflake",
    username="username",
    password="password",
    account="account",
    organization="organization",
    database="database",
    schema="schema",
    description="Snowflake",
    team="Featureform Success Team",
)
table = snowflake.register_table(
    name="transaction",
    variant="final",
    table="Transactions",
    description="Transactions file from Kaggle",
)

@snowflake.sql_transformation(
    variant="variant",
)
def transform():
    """ Get all transactions over $500
    """
    return "SELECT * FROM {{transactions.final}} WHERE amount > 500"

entity = ff.register_entity("user")
redis = ff.register_redis(
    name="redis",
    host="localhost",
    port=1234,
    password="pass",
    db=0,
)

resources = transform.register_resources(
    entity=entity,
    entity_column="abc",
    inference_store=redis,
    features=[
        {"name": "a", "variant": "b", "column": "c", "type": "float32"},
    ],
    labels=[
        {"name": "la", "variant": "lb", "column": "lc", "type": "float32"},
    ],
    timestamp_column="ts",
)

resources.create_training_set(name="ts", variant="v1")
