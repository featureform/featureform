# Inference Store: Fast and Efficient Real-Time Feature Serving

The Inference Store provider, sometimes referred to as the online store, is the component responsible for serving features in real-time for inference purposes. It stores the most recently processed value of each feature, indexed by its associated [Entity](../abstractions/entity). Essentially, the inference store functions as a cache of up-to-date pre-processed feature values, allowing for rapid look-ups. You can access features from the inference store using the `client.features` API as shown below:

```python
client.features([(name, variant)], entities={entity: value})
```

## Types of Feature Serving Methods

### Stream Processed Features for Real-Time Serving
Features created via streaming are automatically updated in the inference store with the most recent values. Additionally, a log of historical feature values is maintained in the offline store to facilitate the construction of point-in-time correct training sets.

### Batch Processed Features for Real-Time Serving
When features are generated through batch transformations, Featureform automatically materializes the most recent values into the inference store for efficient real-time serving.

### On-Demand Features for Real-Time Serving
On-demand features are processed at serving time and cannot be found in the inference store. They are computed on-the-fly to fulfill real-time inference requests.

### Batch Serving Features
Certain models, like lead scoring models, process all features in a batch fashion. In such cases, Featureform serves the features from the offline store to achieve cost-effective and consistent serving.

## Performance Characteristics

Featureform acts as a nearly zero-cost abstraction layer, ensuring that the performance and cost characteristics of your chosen inference store remain similar in magnitude whether you are using Featureform or not. Featureform provides a superior abstraction, seamlessly integrating the inference store with the rest of the feature processing pipeline.
