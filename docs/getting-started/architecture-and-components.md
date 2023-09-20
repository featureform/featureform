# Architecture and Components

[A map of the components that make up Featureform](../.gitbook/assets/architecture-and-components.png)

Featureform's architecture and components are designed to streamline the feature engineering process. It follows a Virtual Feature Store architecture, allowing for pluggable data infrastructure and serving as an overarching application framework for feature definition, management, and serving. Let's explore the key components and interfaces of Featureform's architecture:

## Data Scientist Interface

### Python Framework

The Python framework is the core API for working with Featureform. It offers a declarative approach, enabling data scientists to define and manage resources using Python. You can work with Featureform in various Python environments, such as notebooks, to define your desired state. Featureform takes care of orchestrating the infrastructure to transition it from the current state to the desired state.

### Dashboard / CLI

The dashboard and command-line interface (CLI) serve as user interfaces for Featureform. While the Python framework focuses on resource creation, the dashboard and CLI are designed for resource search, monitoring, and analysis. Users can view, filter, reuse, and analyze existing resources defined in Featureform. Additionally, they can leverage monitoring and alerting features to proactively address issues.

## Model Interfaces

### Inference

The inference interface enables real-time feature serving for machine learning models. It allows models to access the most up-to-date feature values stored in the inference store. Features can be served using the `client.features` API.

### Training

The training interface facilitates the training of machine learning models. Training sets are defined in the Python API, and Featureform builds them with point-in-time correct semantics by joining a label and a set of features. Models can access these training sets in a mini-batch fashion or as a Dataframe via the `client.training_set` API.

## Internal Components

### Resource and Feature Registry

The resource and feature registry is a central component for storing metadata about resources defined in Featureform. It maintains a single source of truth on all of the transformations, features, labels, training sets, and their metadata.

### Search and Discovery

The search and discovery component provides users with the ability to search for and discover existing resources and features. It enables data scientists to find and leverage features created by others, fostering collaboration and reuse.

### Monitoring and Alerting

Monitoring and alerting are critical components for ensuring the reliability and performance of machine learning systems. Featureform's monitoring and alerting capabilities allow users to set clear objectives, monitor model performance, and detect concept drift or model failures.

### Governance and Access Control

*Enterprise Only* To maintain data security and governance, Featureform offers access control mechanisms. Users can define permissions and access policies to restrict who can create, modify, or access resources. This ensures that sensitive data is protected and that only authorized users can make changes.

### Orchestrator

The orchestrator component is responsible for managing the orchestration of infrastructure providers. It coordinates the deployment and execution of various components and resources defined in Featureform.

## Infrastructure Providers

### Offline Store

The offline store serves as the main workhorse for data transformation and storage. It stores primary data sets, transformed data sets, training sets, and more. The offline store is responsible for running transformations, building point-in-time correct training sets, and serving batch-processed features.

### Inference Store

The inference store, also known as the online store, acts as a cache of the most recent feature values for real-time serving. It provides fast lookups for features during inference and supports both streaming and batch-processed features.

### Vector Database

The vector database provider is configured to perform nearest neighbor lookups. It is primarily used for embeddings and enables efficient retrieval of similar items based on vector representations.

### Streaming Data

*Enterprise Only* Featureform has the ability to work with streaming data via a set of streaming providers. They enable Featureform to maintain an inference store of current feature values and maintain logs of historical feature values for use in point-in-time correct training sets.

Featureform's architecture and components work together to simplify the feature engineering process, promote collaboration among data scientists, and ensure the reliability of machine learning systems.
