# Object and File Stores

Object and File Stores serve as fundamental components within the Featureform framework, particularly in the context of ETL-based offline stores like Spark and Pandas on K8s. These stores fulfill two primary functions within Featureform:

1. **Providing Primary Data Sets:** Object and File Stores can contain the primary data sets that Featureform transforms and orchestrates.

2. **Storage for Materialized Data:** These stores are responsible for storing materialized transformations, features, and training sets that are generated as part of the Featureform data processing pipeline.

The combination of these functions enables Featureform's seamless feature processing and serving capabilities.
