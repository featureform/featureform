# Firestore

Featureform supports [Firestore](https://firebase.google.com/docs/firestore) as an Inference Store.

## Implementation

A Firestore document is created for every feature,  within the same collection.  Variants of features have separate documents. Each document maps entities to their feature value. A metadata table is stored in the same Firestore collection as well to allow the provider to keep track of its own state. Featureform's scheduler aims to achieve consistency between Firestore's internal state with the user's desired state as specified in the metadata service.

## Configuration

First we have to add a declarative Firestore configuration in Python. In the following example, only name and credentials are required, but the other parameters are available. Note: "credentials" refers to the file location of the Firestore Configuration (.json).&#x20;

{% code title="firestore_config.py" %}
```python
import featureform as ff
ff.register_firestore(
    name = "firestore",
    description = "Example inference store",
    team = "Featureform",
    collection = "",
    projectID = "",
    credentials = ""
)
```
{% endcode %}

Once our config file is complete, we can apply it to our Featureform deployment

```bash
featureform apply firestore_config.py --host $FEATUREFORM_HOST
```

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry](../getting-started/exploring-the-feature-registry.md).
