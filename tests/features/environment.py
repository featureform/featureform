def before_all(context):
    import urllib.request

    urllib.request.urlretrieve(
        "https://featureform-demo-files.s3.amazonaws.com/transactions_short.csv",
        "transactions.csv",
    )
