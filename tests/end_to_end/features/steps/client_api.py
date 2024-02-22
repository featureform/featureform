from behave import then


@then('I should get the columns for the data source from "{provider}"')
def step_impl(context, provider):
    context.client.apply()
    context.cols = context.client.columns(context.transactions)
    print("\n\n")
    print("ACTUAL COLUMNS: ", context.cols)
    print("\n\n")
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
