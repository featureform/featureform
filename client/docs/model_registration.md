# Model Registration

Although Featureform does not deploy models, it does allow you to tag features with their respective models, as well
as lookup what features are associated with a given model. 

Associating a model with a feature can be done through the serving API during serving time.

```python   
feature = client.features(
    [("avg_user_transactions", "quickstart")],
    {"user_id": "1234"}, 
    model="user_fraud_random_forest"
)
```

Visit the dashboard to see what features are associated with a given model.