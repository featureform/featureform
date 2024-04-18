import os


import featureform as ff
from behave import given, then
from dotenv import load_dotenv

load_dotenv("../../.env")

@then('I should get the columns for the data source from "{provider}"')
def step_impl(context, provider):
    context.client.apply()
    context.cols = context.client.columns(context.transactions)
    if provider == "postgres":
        expected_source_cols = [
            '"transactionid"',
            '"customerid"',
            '"customerdob"',
            '"custlocation"',
            '"custaccountbalance"',
            '"transactionamount"',
            '"timestamp"',
            '"isfraud"',
        ]
    elif provider == "spark":
        expected_source_cols = [
            "TransactionID",
            "CustomerID",
            "CustomerDOB",
            "CustLocation",
            "CustAccountBalance",
            "TransactionAmount",
            "Timestamp",
            "IsFraud",
        ]
    else:
        raise ValueError(f"Unknown provider {provider}")
    assert context.cols == expected_source_cols

@then("I should be able to get spark provider")
def step_impl(context):
    context.spark_provider = ff.get_spark(context.spark_name)
    assert context.spark_provider is not None

@then("I should be able to register transactions_short.csv")
def step_impl(context):
    context.transactions = context.spark_provider.register_file(
        name="transactions_short",
        description="A dataset of transactions",
        file_path="s3://featureform-spark-testing/data/transactions_short.csv",
    )
    context.client.apply()

@then("I should be able to get the data of the resource")
def step_impl(context):
    df = context.client.dataframe(context.transactions)
    assert df is not None
    assert df.count() > 0

@then("I should be able to register a transformation with the source")
def step_impl(context):
    @context.spark_provider.df_transformation(
        inputs=[context.source],
    )
    def transformation(df):
        return df

    context.transformation = transformation
    context.client.dataframe(context.transformation)

@then("I should be able to get the data of the transformation")
def step_impl(context):
    # make it so loop through the transformations? 
    df = context.client.dataframe(context.transformation)
    assert df is not None
    assert df.count() > 0
