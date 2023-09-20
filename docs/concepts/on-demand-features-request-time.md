# Calculating On-Demand Features at Request Time

Certain machine learning predictions rely on data available only at the time of the request. For instance, testing a user transaction for fraud might require data that's passed with the request and cannot be preprocessed. While stream processing offers near real-time features, it can lead to race conditions, potentially rendering the current data unavailable when you access features from the feature store. In such cases, an ideal approach is to compute the required feature at the moment of the request. To achieve this, Featureform exposes an On-Demand Feature API.

An On-Demand Feature represents a Feature resource, rather than a Transformation. It's possible to string together on-demand features; however, On-Demand Features are solely intended for post-processing data during request times.

To define an on-demand feature, employ a decorator that comes with three parameters: a Featureform client to retrieve other necessary features, an entity dictionary serving the same purpose, and an args parameter containing the data for request processing.

```python
import featureform as ff

@ff.ondemand_feature()
def scale_value(client, params, entities):
    return params[0] / 10

client.apply()
```

Once applied, this on-demand feature can be served using the following code snippet:

```python
client.features([scale_value], params=[100])
```

This mechanism enables you to generate features on data that's available in request time. It gives Featureform's API the flexibility to be used to calculate features in all possible contexts.
