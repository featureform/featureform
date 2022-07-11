# DynamoDB

Featureform supports [DynamoDB ](https://aws.amazon.com/dynamodb/)as an Inference Store.

## Implementation

A DynamoDB table is created for every feature. Variants of features have separate documents. Each document maps entities to their feature value.  Each document maps entities to their feature value. A metadata table is stored in DynamoDB as well to allow the provider to keep track of its own state. Featureform's scheduler aims to achieve consistency between DynamoDB's internal state with the user's desired state as specified in the metadata service.

## Configuration

First we have to add a declarative DynamoDB configuration in Python. In the following example, only name, access key, and secret key are required, but the other parameters are available.

{% code title="dynamodb_config.py" %}
```python
import featureform as ff
ff.register_dynamodb(
    name = "dynamodb",
    description = "Example inference store",
    team = "Featureform",
    host = "0.0.0.0",
    port = 8000,
    access_key = "",
    secret_key = ""
)
```
{% endcode %}

Once our config file is complete, we can apply it to our Featureform deployment

```bash
featureform apply dynamodb_config.py --host $FEATUREFORM_HOST
```

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry](../getting-started/exploring-the-feature-registry.md).
