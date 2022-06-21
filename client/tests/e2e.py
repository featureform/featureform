import requests

users = [
    {'name': 'featureformer', 'type': 'User', 'features': None, 'labels': None, 'training-sets': None, 'sources': None,
     'status': 'NO_STATUS'}]

providers = [
    {
        "name": "postgres-quickstart",
        "type": "Provider",
        "description": "A Postgres deployment we created for the Featureform quickstart",
        "provider-type": "POSTGRES_OFFLINE",
        "software": "postgres",
        "team": "",
        "sources": None,
              "features": None, "labels": None, "training-sets": None, "status": "NO_STATUS", "error": ""},
             {"name": "redis-quickstart", "type": "Provider",
              "description": "A Redis deployment we created for the Featureform quickstart",
              "provider-type": "REDIS_ONLINE", "software": "redis", "team": "", "sources": None, "features": None,
              "labels": None, "training-sets": None, "status": "NO_STATUS", "error": ""}]

sources = [{'all-variants': ['quickstart'], 'type': 'Source', 'default-variant': 'quickstart', 'name': 'average_user_transaction', 'variants': {'quickstart': {'created': '2022-06-17T02:12:46.329173178Z', 'description': 'the average transaction amount for a user ', 'name': 'average_user_transaction', 'source-type': 'Transformation', 'owner': 'featureformer', 'provider': 'postgres-quickstart', 'variant': 'quickstart', 'labels': None, 'features': None, 'training-sets': None, 'status': 'READY', 'error': '', 'definition': 'SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id'}}}, {'all-variants': ['kaggle'], 'type': 'Source', 'default-variant': 'kaggle', 'name': 'transactions', 'variants': {'kaggle': {'created': '2022-06-17T02:12:46.130363553Z', 'description': 'Fraud Dataset From Kaggle', 'name': 'transactions', 'source-type': 'Primary Table', 'owner': 'featureformer', 'provider': 'postgres-quickstart', 'variant': 'kaggle', 'labels': None, 'features': None, 'training-sets': None, 'status': 'READY', 'error': '', 'definition': 'Transactions'}}}]

entities =[{'name': 'user', 'type': 'Entity', 'description': '', 'features': None, 'labels': None, 'training-sets': None, 'status': 'NO_STATUS'}]

features = [{'all-variants': ['quickstart'], 'type': 'Feature', 'default-variant': 'quickstart', 'name': 'avg_transactions', 'variants': {'quickstart': {'description': '', 'entity': 'user', 'name': 'avg_transactions', 'owner': 'featureformer', 'provider': 'redis-quickstart', 'data-type': 'float32', 'variant': 'quickstart', 'status': 'READY', 'error': '', 'location': {'Entity': 'user_id', 'Source': '', 'TS': '', 'Value': 'avg_transaction_amt'}, 'source': {'Name': 'average_user_transaction', 'Variant': 'quickstart'}, 'training-sets': None}}}]

labels = [{'all-variants': ['quickstart'], 'type': 'Label', 'default-variant': 'quickstart', 'name': 'fraudulent', 'variants': {'quickstart': {'description': '', 'entity': 'user', 'name': 'fraudulent', 'owner': 'featureformer', 'provider': 'postgres-quickstart', 'data-type': 'bool', 'variant': 'quickstart', 'location': {'Entity': 'customerid', 'Source': '', 'TS': '', 'Value': 'isfraud'}, 'source': {'Name': 'transactions', 'Variant': 'kaggle'}, 'training-sets': None, 'status': 'READY', 'error': ''}}}]

training_sets = [{'all-variants': ['quickstart'], 'type': 'TrainingSet', 'default-variant': 'quickstart', 'name': 'fraud_training', 'variants': {'quickstart': {'description': '', 'name': 'fraud_training', 'owner': 'featureformer', 'provider': 'postgres-quickstart', 'variant': 'quickstart', 'label': {'Name': 'fraudulent', 'Variant': 'quickstart'}, 'features': None, 'status': 'READY', 'error': ''}}}]

def test_user():
    req = requests.get("http://localhost:8080/data/users", json=True)
    json_ret = req.json()
    assert (json_ret == users)


def test_providers():
    req = requests.get("http://localhost:8080/data/providers", json=True)
    json_ret = req.json()
    assert (json_ret == providers)

def test_sources():
    req = requests.get("http://localhost:8080/data/sources", json=True)
    json_ret = req.json()
    assert (json_ret == sources)

def test_entities():
    req = requests.get("http://localhost:8080/data/entities", json=True)
    json_ret = req.json()
    assert (json_ret == entities)

def test_features():
    req = requests.get("http://localhost:8080/data/features", json=True)
    json_ret = req.json()
    for res in json_ret:
        for v in res['variants']:
            del res['variants'][v]['created']
    assert (json_ret == features)

def test_labels():
    req = requests.get("http://localhost:8080/data/labels", json=True)
    json_ret = req.json()
    for res in json_ret:
        for v in res['variants']:
            del res['variants'][v]['created']
    assert (json_ret == labels)

def test_training_sets():
    req = requests.get("http://localhost:8080/data/training-sets", json=True)
    json_ret = req.json()
    for res in json_ret:
        for v in res['variants']:
            del res['variants'][v]['created']
    assert (json_ret == training_sets)