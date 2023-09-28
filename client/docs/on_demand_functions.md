# Ondemand Feature

An Ondemand Feature is a function that can be stored and called at serving time to run arbitrary code. 

## Defining An Ondemand Feature

Ondemand features have three arguments that can be used to pass in parameters, entities, or a serving client to call
other serving functions within itself.

Arguments:

- `client`: The same client that is used to retrieve the ondemand feature with `client.features()`.
- `params`: A list of parameters that are passed in at serving time. This can be any datatype or structure. 
- `entities`: A list of entities that are passed in at serving time. This is optional.


::: featureform.register.Registrar.ondemand_feature
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

## Examples
### Simple Aggregations
Ondemand features can be used to run simple aggregations on values that are passed in as params. In this example, we
will simply be adding two numbers together.

```python
@ff.ondemand_feature(variant="quickstart")
def avg_user_transactions(client, params, entities):
    return params[0] + params[1]


features = client.features([("avg_user_transactions", "quickstart")], params=[1, 2])
print(features)
# [3]

```

### Aggregating Features
Suppose we have a feature that is the sum of two other features. We can use an ondemand feature to aggregate these
internally and return the result. 

```python
@ff.ondemand_feature(variant="quickstart")
def lot_area(client, params, entities):
    """Returns the area of a property in square feet."""
    dim = client.features([("lot_width", "rev_1"), ("lot_length", "rev_1")], entities)
    return dim[0] * dim[1]

# Returns the area of a property of property 1432.
features = client.features([("lot_area", "sqft")], {"property_id": 1432})
print(features)
# [6098]

```

### Nested Ondemand Features
We can call Ondemand Features from within other Ondemand Features. Using the previous example, we can create an
Ondemand feature that converts the area from square feet to square meters.

```python
@ff.ondemand_feature(variant="quickstart")
def lot_area(client, params, entities):
    area = client.features([("lot_area", "sqft")], entities)
    return area[0]/10.764

# Returns the area of a property of property 1432 in square meters.
features = client.features([("lot_area", "m2")], {"property_id": 1432})
print(features)
# [566.5227]

```