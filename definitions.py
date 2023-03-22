import featureform as ff

ff.register_user("featureformer").make_default_owner()

local = ff.register_local()

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

print(average_user_transaction[["CustomerID", "TransactionAmount"]]) 
# OUTPUT (<featureform.register.Registrar object at 0x120f12640>, ('average_user_transaction', 'quickstart'), ['CustomerID', 'TransactionAmount'])

# print("***** VARS ******", vars(average_user_transaction))

# print("***** Subscriptable *****", average_user_transaction["name"])

# print("***** Type ******", type(average_user_transaction))


## BACKWARDS COMPATIBILITY
# user = ff.register_entity("user")

# average_user_transaction.register_resources(
#     entity=user,
#     entity_column="CustomerID",
#     inference_store=local,
#     features=[
#         {"name": "avg_transactions", "variant": "quickstart", "column": "TransactionAmount", "type": "float32"},
#     ],
# )

# print(average_user_transaction[["CustomerID", "TransactionAmount"]])

@ff.entity
class User:
    avg_transactions = ff.Feature(*average_user_transaction[["CustomerID", "TransactionAmount"]])



print(vars(User))
