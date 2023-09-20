# Extending Featureform with Custom Providers and Requesting New Providers

Featureform's architecture is built upon a foundation of provider abstractions, which include Offline Stores, Object/File Stores, Inference Stores, and Vector Databases. Each of these providers adheres to a generic interface, allowing Featureform to seamlessly manage various types of infrastructure. This flexibility is achieved without the need for writing custom code for every specific use case, making it adaptable to heterogeneous infrastructure environments.

Custom Providers in Featureform Enterprise: For Featureform Enterprise users, custom providers have been developed to meet specific needs. In some cases, these custom providers are built to accommodate unique infrastructure requirements.

Featureform's objective is to integrate commonly used infrastructure and providers into its open-source offerings. This ensures that a broader user base can access and utilize these providers to enhance their machine learning workflows.

Requesting New Providers: If there is a specific provider that you require and it is currently missing from Featureform's offerings, or if you need support in building a custom provider integration, you can engage with the Featureform community through [GitHub issues](https://github.com/featureform/featureform) or join the [community Slack](https://join.slack.com/t/featureform-community/shared_invite/zt-xhqp2m4i-JOCaN1vRN2NDXSVif10aQg). This collaborative approach enables the expansion and customization of Featureform to fit into the majority of data infrastructure permutations.
