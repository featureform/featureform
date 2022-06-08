<h1 align="center">
	<img width="300" src="https://raw.githubusercontent.com/featureform/featureform/feature/readme/simba/assets/featureform_logo.png" alt="featureform">
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

<br />
<br />

<img src="https://raw.githubusercontent.com/featureform/featureform/feature/readme/simba/assets/virtual_arch.png" alt="A virtual feature store's architecture" style="width:35em"/>

<br />
<br />


## Features
* **Supported Operations**: Run approximate nearest neighbor lookups, average multiple embeddings, partition tables (spaces), cache locally while training, and more.
* **Storage**: Store and index billions vectors embeddings from our storage layer.
* **Versioning**: Create, manage, and rollback different versions of your embeddings.
* **Access Control**: Encode different business logic and user management directly into Embeddinghub.
* **Monitoring**: Keep track of how embeddings are being used, latency, throughput, and feature drift over time.

<br />


## What is an Feature Store?

Embeddings are dense numerical representations of real-world objects and relationships, expressed as a vector. The vector space quantifies the semantic similarity between categories. Embedding vectors that are close to each other are considered similar. Sometimes, they are used directly for “Similar items to this” section in an e-commerce store. Other times, embeddings are passed to other models. In those cases, the model can share learnings across similar items rather than treating them as two completely unique categories, as is the case with one-hot encodings. For this reason, embeddings can be used to accurately represent sparse data like clickstreams, text, and e-commerce purchases as features to downstream models.

### Further Reading
* [Read up on common embeddings use cases, like recommender systems, nearest neighbor, and natural language processing in our docs.](https://docs.featureform.com)
* [The Definitive Guide to Embeddings](https://www.featureform.com/post/the-definitive-guide-to-embeddings)

<br />
<br />

# Getting Started

## Step 1: Install Embeddinghub client

Install the Python SDK via pip

```
pip install embeddinghub
```

## Step 2: Deploy Docker container ( _optional_ )
The Embeddinghub client can be used without a server. This is useful when using embeddings in a research environment where a database server is not necessary. If that’s the case for you, skip ahead to the next step.

Otherwise, we can use this docker command to run Embeddinghub locally and to map the container's main port to our host's port.

```
docker run featureformcom/embeddinghub -p 7462:7462
```

## Step 3: Initialize Python Client

If you deployed a docker container, you can initialize the python client.

```py
import embeddinghub as eh

hub = eh.connect(eh.Config())
```
Otherwise, you can use a LocalConfig to store and index embeddings locally.

```py
hub = eh.connect(eh.LocalConfig("data/"))
```

## Step 4: Create a Space

Embeddings are written and retrieved from Spaces. When creating a Space we must also specify a version, otherwise a default version is used.

```py
space = hub.create_space("quickstart", dims=3)
```

## Step 5: Upload Embeddings
We will create a dictionary of three embeddings and upload them to our new quickstart space.

```py
embeddings = {
    "apple": [1, 0, 0],
    "orange": [1, 1, 0],
    "potato": [0, 1, 0],
    "chicken": [-1, -1, 0],
}
space.multiset(embeddings)
```

## Step 6: Get nearest neighbors

Now we can compare apples to oranges and get the nearest neighbors.

```py
neighbors = space.nearest_neighbors(key="apple", num=2)
print(neighbors)
```

* [Read through our guide to further explore Featureform's functionality.](https://docs.featureform.com)
* [Explore our docs](https://docs.featureform.com/)

<br />

# Contributing

* To contribute to Embeddinghub, please check out [Contribution docs](https://github.com/featureform/embeddings/blob/main/CONTRIBUTING.md).
* Welcome to our community, join us on [Slack](https://join.slack.com/t/featureform-community/shared_invite/zt-xhqp2m4i-JOCaN1vRN2NDXSVif10aQg).

<br />


# Report Issues

Please help us by [reporting any issues](https://github.com/featureform/featureform/issues/new/choose) you may have while using Featureform.

<br />

# License

* [Mozilla Public License Version 2.0](https://github.com/featureform/featureform/blob/main/LICENSE)
