import featureform as ff
from featureform import local

transactions = local.register_file(
    name="transactions",
    variant="quickstart",
    description="A dataset of fraudulent transactions",
    path="transactions.csv"
)

@local.df_transformation(variant="quickstart",
                         inputs=[("transactions", "quickstart")])
def average_user_transaction(transactions):
    """the average transaction amount for a user """
    return transactions.groupby("CustomerID")["TransactionAmount"].mean()

user = ff.register_entity("user")
# Register a column from our transformation as a feature
average_user_transaction.register_resources(
    entity=user,
    entity_column="CustomerID",
    inference_store=local,
    features=[
        {"name": "avg_transactions", "variant": "quickstart", "column": "TransactionAmount", "type": "float32"},
    ],
)
# Register label from our base Transactions table
transactions.register_resources(
    entity=user,
    entity_column="CustomerID",
    labels=[
        {"name": "fraudulent", "variant": "quickstart", "column": "IsFraud", "type": "bool"},
    ],
)

ff.register_training_set(
    "fraud_training", "quickstart",
    label=("fraudulent", "quickstart"),
    features=[("avg_transactions", "quickstart")],
)