from behave import *
import featureform as ff


@given("we have featureform installed")
def step_impl(context):
    import featureform as ff


@given("we have the transactions file downloaded")
def step_impl(context):
    import os.path

    return os.path.isfile("transactions.csv")


@when("we create a localmode client")
def step_impl(context):
    context.client = ff.Client(local=True)


@when("we register localmode")
def step_impl(context):
    ff.register_local()
    context.client.apply()


@when('we set a run name: "{quickstart}"')
def step_impl(context, quickstart):
    ff.set_run(quickstart)


@when('we register a local file "{file_name}" at path "{path}"')
def step_impl(context, file_name, path):
    context.transactions = ff.local.register_file(
        name=file_name,
        description="A dataset of fraudulent transactions",
        path=path,
    )
    context.client.apply()


@when("we register a transformation")
def step_impl(context):
    @ff.local.df_transformation(inputs=["transactions"])
    def average_user_transaction(transactions):
        """the average transaction amount for a user"""

        return transactions.groupby("CustomerID")["TransactionAmount"].mean()

    context.client.apply()
    context.average_user_transaction = average_user_transaction


@when("we register a feature")
def step_impl(context):
    user = ff.register_entity("user")
    # Register a column from our transformation as a feature
    context.average_user_transaction.register_resources(
        entity=user,
        entity_column="CustomerID",
        features=[
            {
                "name": "avg_transactions",
                "column": "TransactionAmount",
                "type": ff.ScalarType.FLOAT32,
            },
        ],
    )
    context.client.apply()


@when("we register a label")
def step_impl(context):
    user = ff.register_entity("user")
    context.transactions.register_resources(
        entity=user,
        entity_column="CustomerID",
        labels=[
            {
                "name": "fraudulent",
                "column": "IsFraud",
                "type": ff.ScalarType.BOOL,
            },
        ],
    )
    context.client.apply()


@when("we register a training set")
def step_impl(context):
    ff.register_training_set(
        "fraud_training",
        label="fraudulent",
        features=["avg_transactions"],
    )
    context.client.apply()


@then("we should be able to serve a feature")
def step_impl(context):
    fpf = context.client.features(
        [("avg_transactions", ff.get_run())], {"user": "C9088014"}
    )


@then("we should be able to serve a training set")
def step_impl(context):
    training_set = context.client.training_set("fraud_training", ff.get_run())
    i = 0
    for r in training_set:
        print(r)
        i += 1
        if i > 10:
            break
