import pandas as pd
from datetime import datetime
import numpy as np

features_no_ts = {
    'Empty': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [],
        'entity': 'entity',
        'value_col': 'values',
        'expected':
            {
                'entity': [],
                'values': []
            }
    },
    'NoOverlap': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [
            ['a', 1],
            ['b', 2],
            ['c', 3]
        ],
        'entity': 'entity',
        'value_col': 'values',
        'expected':
            {
                'entity': ['a', 'b', 'c'],
                'values': [1, 2, 3]
            }
    },
    'SimpleOverwrite': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [
            ['a', 1],
            ['b', 2],
            ['c', 3],
            ['a', 4]
        ],
        'entity': 'entity',
        'value_col': 'values',
        'expected':
            {
                'entity': ['a', 'b', 'c'],
                'values': [4, 2, 3]
            }
    }

}

features_with_ts = {
    'empty': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [],
        'entity': 'entity',
        'value_col': 'values',
        'ts_col': 'timestamp',
        'expected':
            {
                'entity': [],
                'values': []
            }
    },
    'NoOverlap': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [
            ['a', 1, datetime.fromtimestamp(0)],
            ['b', 2, datetime.fromtimestamp(0)],
            ['c', 3, datetime.fromtimestamp(0)]
        ],
        'entity': 'entity',
        'value_col': 'values',
        'ts_col': 'timestamp',
        'expected':
            {
                'entity': ['a', 'b', 'c'],
                'values': [1, 2, 3],
            }
    },
    'SimpleOverwrite': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [
            ['a', 1, datetime.fromtimestamp(0)],
            ['b', 2, datetime.fromtimestamp(0)],
            ['c', 3, datetime.fromtimestamp(0)],
            ['a', 4, datetime.fromtimestamp(0)]
        ],
        'entity': 'entity',
        'value_col': 'values',
        'ts_col': 'timestamp',
        'expected':
            {
                'entity': ['a', 'b', 'c'],
                'values': [4, 2, 3],
            }
    },
    'SimpleChanges': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [
            ['a', 1, datetime.fromtimestamp(0)],
            ['b', 2, datetime.fromtimestamp(0)],
            ['c', 3, datetime.fromtimestamp(0)],
            ['a', 4, datetime.fromtimestamp(1)]
        ],
        'entity': 'entity',
        'value_col': 'values',
        'ts_col': 'timestamp',
        'expected':
            {
                'entity': ['a', 'b', 'c'],
                'values': [4, 2, 3],
            }
    },
    'OutOfOrderWrites': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [
            ['a', 1, datetime.fromtimestamp(10)],
            ['b', 2, datetime.fromtimestamp(3)],
            ['c', 3, datetime.fromtimestamp(7)],
            ['c', 9, datetime.fromtimestamp(5)],
            ['a', 4, datetime.fromtimestamp(1)]
        ],
        'entity': 'entity',
        'value_col': 'values',
        'ts_col': 'timestamp',
        'expected':
            {
                'entity': ['a', 'b', 'c'],
                'values': [1, 2, 3],
            }
    },
    'OutOfOrderOverwrites': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [
            ['a', 1, datetime.fromtimestamp(10)],
            ['b', 2, datetime.fromtimestamp(3)],
            ['c', 3, datetime.fromtimestamp(7)],
            ['c', 9, datetime.fromtimestamp(5)],
            ['b', 12, datetime.fromtimestamp(2)],
            ['a', 4, datetime.fromtimestamp(1)],
            ['b', 9, datetime.fromtimestamp(3)]
        ],
        'entity': 'entity',
        'value_col': 'values',
        'ts_col': 'timestamp',
        'expected':
            {
                'entity': ['a', 'b', 'c'],
                'values': [1, 9, 3],
            }
    }
}

feature_invalid_entity = {
    'name': "InvalidEntity",
    'columns': ['entity', 'values', 'timestamp'],
    'values': [],
    'entity': 'wrong_entity',
    'value_col': 'values',
    'ts_col': 'timestamp',
}

feature_invalid_value = {
    'name': "InvalidValue",
    'columns': ['entity', 'values', 'timestamp'],
    'values': [],
    'entity': 'entity',
    'value_col': 'wrong_values',
    'ts_col': 'timestamp',
}

feature_invalid_ts = {
    'name': "InvalidTimestamp",
    'columns': ['entity', 'values', 'timestamp'],
    'values': [],
    'entity': 'entity',
    'value_col': 'values',
    'ts_col': 'wrong_timestamp',
}

feature_e2e = {
    'Simple': {
        'columns': ['entity', 'value', 'ts'],
        'values': [
            ['a', 1],
            ['b', 2]
        ],
        'value_cols': ['value'],
        'entity': 'user',
        'entity_loc': 'entity',
        'features': [("avg_transactions", "quickstart")],
        'entities': [("user", "a"), ("user", "b")],
        'expected': np.array([[1], [2]]),
        'ts_col': ""
    },
    'SimpleOverwrite': {
        'columns': ['entity', 'value', 'ts'],
        'values': [
            ['a', 1],
            ['b', 2],
            ['c', 3],
            ['a', 4]
        ],
        'value_cols': ['value'],
        'entity': 'user',
        'entity_loc': 'entity',
        'features': [("avg_transactions", "quickstart")],
        'entities': [("user", "a"), ("user", "b"), ("user", "c")],
        'expected': np.array([[4], [2], [3]]),
        'ts_col': ""
    },
    'SimpleChanges': {
        'columns': ['entity', 'value', 'ts'],
        'values': [
            ['a', 1, datetime.fromtimestamp(0)],
            ['b', 2, datetime.fromtimestamp(0)],
            ['c', 3, datetime.fromtimestamp(0)],
            ['a', 4, datetime.fromtimestamp(1)]
        ],
        'value_cols': ['value'],
        'entity': 'user',
        'entity_loc': 'entity',
        'features': [("avg_transactions", "quickstart")],
        'entities': [("user", "a"), ("user", "b"), ("user", "c")],
        'expected': np.array([[4], [2], [3]]),
        'ts_col': "ts"
    },
    'OutOfOrderWrites': {
        'columns': ['entity', 'value', 'ts'],
        'values': [
            ['a', 1, datetime.fromtimestamp(10)],
            ['b', 2, datetime.fromtimestamp(3)],
            ['c', 3, datetime.fromtimestamp(7)],
            ['c', 9, datetime.fromtimestamp(5)],
            ['a', 4, datetime.fromtimestamp(1)]
        ],
        'value_cols': ['value'],
        'entity': 'user',
        'entity_loc': 'entity',
        'features': [("avg_transactions", "quickstart")],
        'entities': [("user", "a"), ("user", "b"), ("user", "c")],
        'expected': np.array([[1], [2], [3]]),
        'ts_col': "ts"
    },
    'OutOfOrderOverwrites': {
        'columns': ['entity', 'value', 'ts'],
        'values': [
            ['a', 1, datetime.fromtimestamp(10)],
            ['b', 2, datetime.fromtimestamp(3)],
            ['c', 3, datetime.fromtimestamp(7)],
            ['c', 9, datetime.fromtimestamp(5)],
            ['b', 12, datetime.fromtimestamp(2)],
            ['a', 4, datetime.fromtimestamp(1)],
            ['b', 9, datetime.fromtimestamp(3)]
        ],
        'value_cols': ['value'],
        'entity': 'user',
        'entity_loc': 'entity',
        'features': [("avg_transactions", "quickstart")],
        'entities': [("user", "a"), ("user", "b"), ("user", "c")],
        'expected': np.array([[1], [9], [3]]),
        'ts_col': "ts"
    },
    'MultipleFeatures': {
        'columns': ['entity', 'value1', 'value2'],
        'values': [
            ['a', 'one', 1],
            ['b', 'two', 2]
        ],
        'value_cols': ['value1', 'value2'],
        'entity': 'user',
        'entity_loc': 'entity',
        'features': [("avg_transactions", "quickstart"), ("avg_transactions", "quickstart2")],
        'entities': [("user", "a"), ("user", "b")],
        'expected': np.array([['one', 1], ['two', 2]]),
        'ts_col': ""
    },
    'MultipleFeaturesWithTS': {
        'columns': ['entity', 'value1', 'value2', 'ts'],
        'values': [
            ['a', 'one', 1, datetime.fromtimestamp(0)],
            ['b', 'two', 2, datetime.fromtimestamp(0)]
        ],
        'value_cols': ['value1', 'value2'],
        'entity': 'user',
        'entity_loc': 'entity',
        'features': [("avg_transactions", "quickstart"), ("avg_transactions", "quickstart2")],
        'entities': [("user", "a"), ("user", "b")],
        'expected': np.array([['one', 1], ['two', 2]]),
        'ts_col': "ts"
    },
    'MultipleFeaturesChanges': {
        'columns': ['entity', 'value1', 'value2', 'ts'],
        'values': [
            ['a', 'one', 1, datetime.fromtimestamp(0)],
            ['b', 'two', 2, datetime.fromtimestamp(0)],
            ['c', 'three', 3, datetime.fromtimestamp(0)],
            ['a', 'four', 4, datetime.fromtimestamp(1)]
        ],
        'value_cols': ['value1', 'value2'],
        'entity': 'user',
        'entity_loc': 'entity',
        'features': [("avg_transactions", "quickstart"), ("avg_transactions", "quickstart2")],
        'entities': [("user", "a"), ("user", "b")],
        'expected': np.array([['four', 4], ['two', 2]]),
        'ts_col': "ts"
    },
}
