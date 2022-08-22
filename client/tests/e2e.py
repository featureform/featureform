import time

import pytest
import requests

MAX_RETRIES = 20
RETRY_WAIT = 5

users = [
    {
        'name': 'featureformer',
        'type': 'User',
        'features': None,
        'labels': None,
        'training-sets': None,
        'sources': None,
        'status': 'NO_STATUS'
    }
]

providers = [
    {
        "name": "postgres-quickstart",
        "type": "Provider",
        "description": "A Postgres deployment we created for the Featureform quickstart",
        "provider-type": "POSTGRES_OFFLINE",
        "software": "postgres",
        "team": "",
        "sources": None,
        "features": None, "labels": None, "training-sets": None, "status": "NO_STATUS", "error": ""
    },
    {
        "name": "redis-quickstart",
        "type": "Provider",
        "description": "A Redis deployment we created for the Featureform quickstart",
        "provider-type": "REDIS_ONLINE",
        "software": "redis",
        "team": "",
        "sources": None,
        "features": None,
        "labels": None,
        "training-sets": None,
        "status": "NO_STATUS",
        "error": ""}
]

sources = [
    {
        'all-variants': ['quickstart'],
        'type': 'Source',
        'default-variant': 'quickstart',
        'name': 'average_user_transaction',
        'variants': {
            'quickstart': {
                'description': 'the average transaction amount for a user ',
                'name': 'average_user_transaction',
                'source-type': 'Transformation',
                'owner': 'featureformer',
                'provider': 'postgres-quickstart',
                'variant': 'quickstart',
                'labels': None,
                'features': None,
                'training-sets': None,
                'status': 'READY',
                'error': '',
                'definition': 'SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id'
            }
        }
    },
    {
        'all-variants': ['kaggle'],
        'type': 'Source',
        'default-variant': 'kaggle',
        'name': 'transactions',
        'variants': {
            'kaggle': {
                'description': 'Fraud Dataset From Kaggle',
                'name': 'transactions',
                'source-type': 'Primary Table',
                'owner': 'featureformer',
                'provider': 'postgres-quickstart',
                'variant': 'kaggle',
                'labels': None,
                'features': None,
                'training-sets': None,
                'status': 'READY',
                'error': '',
                'definition': 'Transactions'
            }
        }
    }
]

entities = [
    {
        'name': 'user',
        'type': 'Entity',
        'description': '',
        'features': None,
        'labels': None,
        'training-sets': None,
        'status': 'NO_STATUS'
    }
]

features = [
    {
        'all-variants': ['quickstart'],
        'type': 'Feature',
        'default-variant': 'quickstart',
        'name': 'avg_transactions',
        'variants': {
            'quickstart': {
                'description': '',
                'entity': 'user',
                'name': 'avg_transactions',
                'owner': 'featureformer',
                'provider': 'redis-quickstart',
                'data-type': 'float32',
                'variant': 'quickstart',
                'status': 'READY',
                'error': '',
                'location': {
                    'Entity': 'user_id',
                    'Source': '',
                    'TS': '',
                    'Value': 'avg_transaction_amt'
                },
                'source': {
                    'Name': 'average_user_transaction',
                    'Variant': 'quickstart'
                },
                'training-sets': None
            }
        }
    }
]

labels = [
    {
        'all-variants': ['quickstart'],
        'type': 'Label',
        'default-variant': 'quickstart',
        'name': 'fraudulent',
        'variants': {
            'quickstart': {
                'description': '',
                'entity': 'user',
                'name': 'fraudulent',
                'owner': 'featureformer',
                'provider': 'postgres-quickstart',
                'data-type': 'bool',
                'variant': 'quickstart',
                'location': {
                    'Entity': 'customerid',
                    'Source': '',
                    'TS': '',
                    'Value': 'isfraud'
                },
                'source': {
                    'Name': 'transactions',
                    'Variant': 'kaggle'
                },
                'training-sets': None,
                'status': 'READY',
                'error': ''
            }
        }
    }
]

training_sets = [
    {
        'all-variants': ['quickstart'],
        'type': 'TrainingSet',
        'default-variant': 'quickstart',
        'name': 'fraud_training',
        'variants': {
            'quickstart': {
                'description': '',
                'name': 'fraud_training',
                'owner': 'featureformer',
                'provider': 'postgres-quickstart',
                'variant': 'quickstart',
                'label': {
                    'Name': 'fraudulent',
                    'Variant': 'quickstart'
                }, 'features': None,
                'status': 'READY',
                'error': ''
            }
        }
    }
]


def test_user():
    req = requests.get("http://localhost:7000/data/users", json=True)
    json_ret = req.json()
    assert (json_ret == users)


def test_providers():
    req = requests.get("http://localhost:7000/data/providers", json=True)
    json_ret = req.json()
    assert (json_ret == providers)


def test_sources():
    check_results("http://localhost:7000/data/sources", sources)


def test_entities():
    req = requests.get("http://localhost:7000/data/entities", json=True)
    json_ret = req.json()
    assert (json_ret == entities)


def test_features():
    check_results("http://localhost:7000/data/features", features)


def test_labels():
    check_results("http://localhost:7000/data/labels", labels)


def test_training_sets():
    check_results("http://localhost:7000/data/training-sets", training_sets)


def check_results(endpoint, expected):
    retries = 0
    while 1:
        req = requests.get(endpoint, json=True)
        json_ret = req.json()
        filtered = remove_timestamps(json_ret)
        if retries > MAX_RETRIES:
            raise Exception("Timed out waiting for data to be ready", json_ret)
        if is_ready(filtered):
            break
        else:
            retries += 1
            time.sleep(RETRY_WAIT)

    assert (json_ret == expected)


def remove_timestamps(json_value):
    for res in json_value:
        for v in res['variants']:
            del res['variants'][v]['created']
    return json_value


def is_ready(json_value):
    ready = True
    for res in json_value:
        for v in res['variants']:
            if res['variants'][v]['status'] != 'READY':
                ready = False

    return ready
