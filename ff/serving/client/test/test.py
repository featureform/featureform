import register as ff

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
