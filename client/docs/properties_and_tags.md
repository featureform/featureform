# Properties and Tags

Every resource in FeatureForm has a set of properties and tags that can be used to filter and search for resources.

To add a tag to a resource, simply add a `tags` field to the resource definition. This field should be a list of strings.

To add a property to a resource, simply add a `properties` field to the resource definition. 
This field should be a dictionary of strings to strings.

## Example

```python

transactions = postgres.register_table(
    name="transactions",
    table="transactions_table",
    tags=["finance"],
    properties={"department": "fraud_detection"},
)

```

## Behavior

- Tags and properties are case-sensitive.
- Reapplying with new tags will append the new tags and ignore duplicates.
- Reapplying with new properties will append the new properties and overwrite existing properties with the same key.
- Deletion can be done in the dashboard.