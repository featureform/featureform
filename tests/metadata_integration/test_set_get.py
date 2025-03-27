import pytest
import featureform as ff

@pytest.fixture
def client():
    client = ff.Client(host="localhost:7878", insecure=True)
    yield client
    ff.clear_state()


def test_e2e_register_and_apply(client):

    default_variant = "quickstart"

    postgres = ff.register_postgres(
        name="postgres-quickstart",
        host="host.docker.internal",
        port="5432",
        user="postgres",
        password="password",
        database="postgres",
    )

    redis = ff.register_redis(
        name="redis-quickstart",
        host="host.docker.internal",
        port=6379,
        password="",
    )

    transactions = postgres.register_table(
        name="transactions",
        table="transactions",
    )

    @postgres.sql_transformation(inputs=[transactions])
    def average_user_transaction(tr):
        return (
            "SELECT CustomerID as user_id, avg(TransactionAmount) "
            "as avg_transaction_amt from {{tr}} GROUP BY user_id"
        )

    @ff.entity
    class User:
        avg_transactions = ff.Feature(
            average_user_transaction[
                ["user_id", "avg_transaction_amt"]
            ],  # We can optional include the `timestamp_column` "timestamp" here
            variant=default_variant,
            type=ff.Float32,
            inference_store=redis,
        )

        fraudulent = ff.Label(
            transactions[["customerid", "isfraud"]], 
            variant=default_variant, 
            type=ff.Bool,
        )


    ff.register_training_set(
        name="fraud_training",
        label=User.fraudulent,
        features=[User.avg_transactions],
        variant=default_variant,
    )
    client.apply(asynchronous=True)

    # pull the various resources
    features = client.list_features()
    labels = client.list_labels()
    training_sets = client.list_training_sets()

       # all the resources are present: features, labels, and training_sets
    assert len(features) == 1
    assert features[0].name == "avg_transactions"
    assert features[0].default_variant == default_variant
    assert features[0].variants == [default_variant]

    assert len(labels) == 1
    assert labels[0].name == "fraudulent"
    assert labels[0].default_variant == default_variant
    assert labels[0].variants == [default_variant]
    
    assert len(training_sets) == 1
    assert training_sets[0].name == "fraud_training"
    assert training_sets[0].default_variant == default_variant
    assert training_sets[0].variants == [default_variant]


    # both postgres and redis should be avaible via get* methods
    assert postgres == client.get_postgres("postgres-quickstart")
    assert redis == client.get_redis("redis-quickstart")

 