---
description: >-
  A quick start guide for Featureform localmode. This is not meant for
  production, but rather for local experimentation or to test our the
  Featureform API.
---

# Quickstart (Local)

## Step 1: Install Featureform

### Requirements

- Python 3.7+

Install the Featureform SDK via Pip.

```
pip install featureform
```


## Step 2: Download test data

For this quickstart, we'll use a fraudulent transaction dataset that can be found here: [https://featureform-demo-files.s3.amazonaws.com/transactions.csv](https://featureform-demo-files.s3.amazonaws.com/transactions.csv)\
\
The data contains 9 columns, almost all of would require some feature engineering before using in a typical model.

```
TransactionID,CustomerID,CustomerDOB,CustLocation,CustAccountBalance,TransactionAmount (INR),Timestamp,IsFraud
T1,C5841053,10/1/94,JAMSHEDPUR,17819.05,25,2022-04-09 11:33:09,False
T2,C2142763,4/4/57,JHAJJAR,2270.69,27999,2022-03-27 01:04:21,False
T3,C4417068,26/11/96,MUMBAI,17874.44,459,2022-04-07 00:48:14,False
T4,C5342380,14/9/73,MUMBAI,866503.21,2060,2022-04-14 07:56:59,True
T5,C9031234,24/3/88,NAVI MUMBAI,6714.43,1762.5,2022-04-13 07:39:19,False
T6,C1536588,8/10/72,ITANAGAR,53609.2,676,2022-03-26 17:02:51,True
T7,C7126560,26/1/92,MUMBAI,973.46,566,2022-03-29 08:00:09,True
T8,C1220223,27/1/82,MUMBAI,95075.54,148,2022-04-12 07:01:02,True
T9,C8536061,19/4/88,GURGAON,14906.96,833,2022-04-10 20:43:10,True
```

## Step 3: Register files

We can write a config file in Python that registers our test data file.

{% code title="definitions.py" %}
```python
import featureform as ff

ff.register_user("featureformer").make_default_owner()

local = ff.register_local()

transactions = local.register_file(
    name="transactions",
    variant="quickstart",
    description="A dataset of fraudulent transactions",
    path="transactions.csv"
)
```
{% endcode %}

Next, we'll define a Dataframe transformation on our dataset.

{% code title="definitions.py" %}
```python
@local.df_transformation(variant="quickstart",
                         inputs=[("transactions", "quickstart")])
def average_user_transaction(transactions):
    """the average transaction amount for a user """
    return transactions.groupby("CustomerID")["TransactionAmount"].mean()
```
{% endcode %}

Next, we'll register a passenger entity to associate with a feature and label.

{% code title="definitions.py" %}
```python
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
```
{% endcode %}

Finally, we'll join together the feature and label intro a training set.

{% code title="definitions.py" %}
```python
ff.register_training_set(
    "fraud_training", "quickstart",
    label=("fraudulent", "quickstart"),
    features=[("avg_transactions", "quickstart")],
)
```
{% endcode %}

Now that our definitions are complete, we can apply it to our Featureform instance.

```bash
featureform apply definitions.py --local
```

## Step 4: Serve features for training and inference

Once we have our training set and features registered, we can train our model.

```python
import featureform as ff

client = ff.ServingLocalClient()
dataset = client.training_set("fraud_training", "quickstart")
training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
for feature_batch in training_dataset:
    # Train model
```

We can serve features in production once we deploy our trained model as well.

```python
import featureform as ff

client = ff.ServingLocalClient()
fpf = client.features([("avg_transactions", "quickstart")], {"CustomerID", "C1410926"})
# Run features through model
```

{% hint style="info" %}
In Local Mode we use ServingLocalClient instead of ServingClient.
{% endhint %}
