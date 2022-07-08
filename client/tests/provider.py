import os
import featureform as ff


def test_dynamo():
    client = ff.ResourceClient("localhost:8000", False)
    client.register_dynamodb(
        name="test-dynamo",
        description="Test of dynamo-db creation",
        team="featureform",
        access_key=os.getenv("DYNAMO_ACCESS_KEY"),
        secret_key=os.getenv("DYNAMO_SECRET_KEY")
    )
    client.apply()
