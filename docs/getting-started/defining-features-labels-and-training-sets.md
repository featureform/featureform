# Defining Features, Labels, and Training Sets

Once we have our fully transformed sources, we can register their columns as features and labels. We can then join a label with a set of features to create a training set.

## Registering Entities

Every feature must describe an entity. An entity can be thought of as a primary key table, and every feature must have at least a single foreign key entity field. Common entities include users, items, and purchases. Entities can be anything that a feature can describe.

```python
passenger = ff.register_entity("passenger")
```

## Registering Features and Labels

Once our entities are specified, we can begin to associate features and labels with them. Features and labels are each made up of at least two columns, an entity column and a value column. Features and labels that change value over time should be linked to a timestamp column as well. The timestamp column also allows us to create [point-in-time correct training data](defining-features-labels-and-training-sets.md#point-in-time-correctness).

```python
# Register a column from a transformation as a feature
fare_per_family_member.register_resources(
    entity=passenger,
    entity_column="PassengerID",
    inference_store=redis,
    features=[
        {"name": "fpf", "variant": "quickstart", "column": "Fare / Parch", "type": "float64"},
    ],
)
# Register label from the original file
titanic.register_resources(
    entity=passenger,
    entity_column="PassengerID",
    labels=[
        {"name": "survived", "variant": "quickstart", "column": "Survived", "type": "int"},
    ],
)
```

## Registering Training Sets

Once we have our features and labels registered, we can create a training set. Training set creation works by joining a label with a set of features via their entity value and timestamp. For each row of the label, the entity value is used to look up all of the feature values in the training set. When a timestamp is included in the label and the feature, the training set will contain the latest feature value where the feature's timestamp is less than the label's.

```python
ff.register_training_set(
    "titanic_training", "quickstart",
    label=("survived", "quickstart"),
    features=[("fpf", "quickstart")],
)
```

### Point-in-Time Correctness

Training sets are point-in-time correct. To illustrate point-in-time correctness, image that we are creating a training set from the fraud label previewed below:

| Transaction | User | Fraudulent Charge | Timestamp   |
| ----------- | ---- | ----------------- | ----------- |
| 1           | A    | False             | Jan 3, 2022 |
| 2           | A    | True              | Jan 5, 2022 |

And a feature specifying the user's average purchase price:

| User | Avg Purchase Price | Timestamp   |
| ---- | ------------------ | ----------- |
| A    | 5                  | Jan 2, 2022 |
| A    | 10                 | Jan 4, 2022 |

The training set would look like this:

| Avg Purchase Price | Fraudulent Charge |
| ------------------ | ----------------- |
| 5                  | False             |
| 10                 | True              |

Notice that the first row's feature value is 5, while the second row's feature value is 10. That's because at the time of the first label, Jan 3rd, 2022, the feature's value was 5. On Jan 5th, 2022, the feature's value was 10.
