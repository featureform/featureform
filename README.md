<h1 align="center">
	<img width="300" src="https://raw.githubusercontent.com/featureform/featureform/main/assets/featureform_logo.png" alt="featureform">
	<br>
</h1>

<div align="center">
	<a href="https://github.com/featureform/featureform/actions"><img src="https://img.shields.io/badge/featureform-workflow-blue?style=for-the-badge&logo=appveyor" alt="Embedding Store workflow"></a>
    <a href="https://join.slack.com/t/featureform-community/shared_invite/zt-xhqp2m4i-JOCaN1vRN2NDXSVif10aQg" target="_blank"><img src="https://img.shields.io/badge/Join-Slack-blue?style=for-the-badge&logo=appveyor" alt="Featureform Slack"></a>
    <br>
    <a href="https://www.python.org/downloads/" target="_blank"><img src="https://img.shields.io/badge/python-3.6%20|%203.7|%203.8-brightgreen.svg" alt="Python supported"></a>
    <a href="https://pypi.org/project/featureform/" target="_blank"><img src="https://badge.fury.io/py/featureform.svg" alt="PyPi Version"></a>
    <a href="https://www.featureform.com/"><img src="https://img.shields.io/website?url=https%3A%2F%2Fwww.featureform.com%2F?style=for-the-badge&logo=appveyor" alt="Featureform Website"></a>  
    <a href="https://codecov.io/gh/featureform/featureform"><img src="https://codecov.io/gh/featureform/featureform/branch/main/graph/badge.svg?token=3GP8NVYT7Y"/></a>
    <a href="https://twitter.com/featureformML" target="_blank"><img src="https://img.shields.io/twitter/url/http/shields.io.svg?style=social" alt="Twitter"></a>


	
</div>

<div align="center">
    <h3 align="center">
        <a href="https://www.featureform.com/">Website</a>
        <span> | </span>
        <a href="https://docs.featureform.com/">Docs</a>
        <span> | </span>
        <!-- <a href="https://apidocs.featureform.com/">API Docs</a>
        <span> | </span> -->
        <a href="https://join.slack.com/t/featureform-community/shared_invite/zt-xhqp2m4i-JOCaN1vRN2NDXSVif10aQg">Community forum</a>
    </h3>
</div>


# What is Featureform?


[Featureform](https://featureform.com) is a virtual feature store. It enables data scientists to define, manage, and serve their ML model's features. Featureform sits atop your existing infrastructure and orchestrates it to work like a traditional feature store.
By using Featureform, a data science team can solve the organizational problems:

* **Enhance Collaboration** Featureform ensures that transformations, features, labels, and training sets are defined in a standardized form, so they can easily be shared, re-used, and understood across the team.
* **Organize Experimentation** The days of untitled_128.ipynb are over. Transformations, features, and training sets can be pushed from notebooks to a centralized feature repository with metadata like name, variant, lineage, and owner.
* **Facilitate Deployment** Once a feature is ready to be deployed, Featureform will orchestrate your data infrastructure to make it ready in production. Using the Featureform API, you won't have to worry about the idiosyncrasies of your heterogeneous infrastructure (beyond their transformation language).
* **Increase Reliability** Featureform enforces that all features, labels, and training sets are immutable. This allows them to safely be re-used among data scientists without worrying about logic changing. Furthermore, Featureform's orchestrator will handle retry logic and attempt to resolve other common distributed system problems automatically.
* **Preserve Compliance** With built-in role-based access control, audit logs, and dynamic serving rules, your compliance logic can be enforced directly by Featureform.

### Further Reading
* [Feature Stores Explained: The Three Common Architectures](https://www.featureform.com/post/feature-stores-explained-the-three-common-architectures)


<br />
<br />

<img src="https://raw.githubusercontent.com/featureform/featureform/main/assets/virtual_arch.png" alt="A virtual feature store's architecture" style="width:50em"/>

<br />
<br />

# Why is Featureform unique?
**Use your existing data infrastructure.** Featureform does not replace your existing infrastructure. Rather, Featureform transforms your existing infrastructure into a feature store. In being infrastructure-agnostic, teams can pick the right data infrastructure to solve their processing problems, while Featureform provides a feature store abstraction above it. Featureform orchestrates and manages transformations rather than actually computing them. The computations are offloaded to the organization's existing data infrastructure. In this way, Featureform is more akin to a framework and workflow, than an additional piece of data infrastructure.

**Designed for both single data scientists and large enterprise teams** Whether you're a single data scientist or a part of a large enterprise organization, Featureform allows you to document and push your transformations, features, and training sets definitions to a centralized repository. It works everywhere from a laptop to a large heterogeneous cloud deployment.
* _A single data scientist working locally_: The days of untitled_128.ipynb, df_final_final_7, and hundreds of undocumented versions of datasets. A data scientist working in a notebook can push transformation, feature, and training set definitions to a centralized, local repository.
* _A single data scientist with a production deployment_: Register your PySpark transformations and let Featureform orchestrate your data infrastructure from Spark to Redis, and monitor both the infrastructure and the data.
* _A data science team_: Share, re-use, and learn from each other's transformations, features, and training sets. Featureform standardizes how machine learning resources are defined and provides an interface for search and discovery. It also maintains a history of changes, allows for different variants of features, and enforces immutability to resolve the most common cases of failure when sharing resources.
* _A data science organization_: An enterprise will have a variety of different rules around access control of their data and features. The rules may be based on the data scientist’s role, the model’s category, or dynamically based on a user’s input data (i.e. they are in Europe and subject to GDPR). All of these rules can be specified, and Featureform will enforce them. Data scientists can be sure to comply with the organization’s governance rules without modifying their workflow.

**Native embeddings support** Featureform was built from the ground up with embeddings in mind. It supports vector databases as both inference and training stores. Transformer models can be used as transformations, so that embedding tables can be versioned and reliably regenerated. We even created and open-sourced a popular vector database, Emeddinghub.

**Open-source** Featureform is free to use under the [Mozilla Public License 2.0](https://github.com/featureform/featureform/blob/main/LICENSE).

<br />

# The Featureform Abstraction

<br />
<br />

<img src="https://raw.githubusercontent.com/featureform/featureform/main/assets/components.svg" alt="The components of a feature" style="width:50em"/>

<br />
<br />

In reality, the feature’s definition is split across different pieces of infrastructure: the data source, the transformations, the inference store, the training store, and all their underlying data infrastructure. However, a data scientist will think of a feature in its logical form, something like: “a user’s average purchase price”. Featureform allows data scientists to define features in their logical form through transformation, providers, label, and training set resources. Featureform will then orchestrate the actual underlying components to achieve the data scientists' desired state.

# How to use Featureform

Featureform can be run locally on files or in Kubernetes with your existing infrastructure. To check out how to run it in the cloud or on minikube,
follow our Kubernetes quickstart [here](https://docs.featureform.com/quickstart-kubernetes).

Otherwise, continue below to see how to run Featureform locally.
## Install Featureform
```
pip install featureform
```

## Download sample data
We'll use a fraudulent transaction dataset that can be found here: https://featureform-demo-files.s3.amazonaws.com/transactions.csv

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

## Register local provider
We can write a config file in Python that registers our test data file.

```py
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
Next, we'll define a Dataframe transformation on our dataset.

```python
@local.df_transformation(variant="quickstart",
                         inputs=[("transactions", "quickstart")])
def average_user_transaction(transactions):
    """the average transaction amount for a user """
    return transactions.groupby("CustomerID")["TransactionAmount"].mean()
```

Next, we'll register a passenger entity to associate with a feature and label.

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

Finally, we'll join together the feature and label intro a training set.
```python
ff.register_training_set(
"fraud_training", "quickstart",
label=("fraudulent", "quickstart"),
features=[("avg_transactions", "quickstart")],
)
```
Now that our definitions are complete, we can apply it to our Featureform instance.

```
featureform apply definitions.py --local
```

## Serve features for training and inference
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
fpf = client.features([("avg_transactions", "quickstart")], ("CustomerID", "C1410926"))
# Run features through model
```

## Explore the feature registry

We can use the feature registry to search, monitor, and discover our machine learning resources.

<br />
<br />

<img src="https://raw.githubusercontent.com/featureform/featureform/main/assets/featureform-dash.png" alt="The feature store registry" style="width:50em"/>

<br />
<br />

# Contributing

* To contribute to Embeddinghub, please check out [Contribution docs](https://github.com/featureform/featureform/blob/main/CONTRIBUTING.md).
* Welcome to our community, join us on [Slack](https://join.slack.com/t/featureform-community/shared_invite/zt-xhqp2m4i-JOCaN1vRN2NDXSVif10aQg).

<br />


# Report Issues

Please help us by [reporting any issues](https://github.com/featureform/featureform/issues/new/choose) you may have while using Featureform.

<br />

# License

* [Mozilla Public License Version 2.0](https://github.com/featureform/featureform/blob/main/LICENSE)
