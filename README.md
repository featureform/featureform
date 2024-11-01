<h1 align="center">
	<img width="300" src="https://raw.githubusercontent.com/featureform/featureform/main/assets/featureform_logo.png" alt="featureform">
	<br>
</h1>

<div align="center">
	<a href="https://github.com/featureform/featureform/actions"><img src="https://img.shields.io/badge/featureform-workflow-blue?style=for-the-badge&logo=appveyor" alt="Embedding Store workflow"></a>
    <a href="https://join.slack.com/t/featureform-community/shared_invite/zt-xhqp2m4i-JOCaN1vRN2NDXSVif10aQg" target="_blank"><img src="https://img.shields.io/badge/Join-Slack-blue?style=for-the-badge&logo=appveyor" alt="Featureform Slack"></a>
    <br>
    <a href="https://www.python.org/downloads/" target="_blank"><img src="https://img.shields.io/badge/python-%203.7|3.8|3.9|3.10-brightgreen.svg" alt="Python supported"></a>
    <a href="https://pypi.org/project/featureform/" target="_blank"><img src="https://badge.fury.io/py/featureform.svg" alt="PyPi Version"></a>
    <a href="https://www.featureform.com/"><img src="https://img.shields.io/website?url=https%3A%2F%2Fwww.featureform.com%2F?style=for-the-badge&logo=appveyor" alt="Featureform Website"></a>  
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
By using Featureform, a data science team can solve the following organizational problems:

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

In reality, the feature’s definition is split across different pieces of infrastructure: the data source, the transformations, the inference store, the training store, and all their underlying data infrastructure. However, a data scientist will think of a feature in its logical form, something like: “a user’s average purchase price”. Featureform allows data scientists to define features in their logical form through transformations, providers, labels, and training set resources. Featureform will then orchestrate the actual underlying components to achieve the data scientists' desired state.

# How to use Featureform
Featureform can be run locally on files or in Kubernetes with your existing infrastructure.
## Kubernetes

Featureform on Kubernetes can be used to connect to your existing cloud infrastructure and can also be run 
locally on Minikube. 

To check out how to run it in the cloud,
follow our [Kubernetes deployment](https://docs.featureform.com/deployment/kubernetes).

To try Featureform in a single docker container, follow our [docker quickstart guide](https://docs.featureform.com/deployment/quickstart-docker)


<br />
<br />

# Contributing

* To contribute to Featureform, please check out [Contribution docs](https://github.com/featureform/featureform/blob/main/CONTRIBUTING.md).
* Welcome to our community, join us on [Slack](https://join.slack.com/t/featureform-community/shared_invite/zt-xhqp2m4i-JOCaN1vRN2NDXSVif10aQg).

<br />


# Report Issues

Please help us by [reporting any issues](https://github.com/featureform/featureform/issues/new/choose) you may have while using Featureform.

<br />

# License

* [Mozilla Public License Version 2.0](https://github.com/featureform/featureform/blob/main/LICENSE)
