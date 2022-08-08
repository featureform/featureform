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
        'entities': [{"user": "a"}, {"user": "b"}],
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
        'entities': [{"user": "a"}, {"user": "b"}, {"user": "c"}],
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
        'entities': [{"user": "a"}, {"user": "b"}, {"user": "c"}],
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
        'entities': [{"user": "a"}, {"user": "b"}, {"user": "c"}],
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
        'entities': [{"user": "a"}, {"user": "b"}, {"user": "c"}],
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
        'entities': [{"user": "a"}, {"user": "b"}],
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
        'entities': [{"user": "a"}, {"user": "b"}],
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
        'entities': [{"user": "a"}, {"user": "b"}],
        'expected': np.array([['four', 4], ['two', 2]]),
        'ts_col': "ts"
    },
}

labels = {
    'Empty': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [],
        'entity_name': 'entity',
        'entity_col': 'entity',
        'value_col': 'values',
        'expected': pd.DataFrame({'entity':[], 'values':[]}, dtype='object'),
        'ts_col': ""
    },
    'Simple': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [
            ['a', 1],
            ['b', 2]
        ],
        'entity_name': 'entity',
        'entity_col': 'entity',
        'value_col': 'values',
        'expected': pd.DataFrame(
            {
                'entity':['a', 'b'],
                'values':[1, 2]
            }
        ),
        'ts_col': ""
    },
    'DifferentEntityName': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [
            ['a', 1],
            ['b', 2]
        ],
        'entity_name': 'user',
        'entity_col': 'entity',
        'value_col': 'values',
        'expected': pd.DataFrame(
            {
                'user':['a', 'b'],
                'values':[1, 2]
            }
        ),
        'ts_col': ""
    },
    'WithTimestamp': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [
            ['a', 1, datetime.fromtimestamp(0)],
            ['b', 2, datetime.fromtimestamp(0)]
        ],
        'entity_name': 'entity',
        'entity_col': 'entity',
        'value_col': 'values',
        'expected': pd.DataFrame(
            {
                'entity': ['a', 'b'],
                'values': [1, 2],
                'timestamp': [datetime.fromtimestamp(0), datetime.fromtimestamp(0)]
            }
        ),
        'ts_col': "timestamp"
    },
    'WithSameTimestamp': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [
            ['a', 1, datetime.fromtimestamp(0)],
            ['b', 2, datetime.fromtimestamp(0)],
            ['a', 3, datetime.fromtimestamp(0)]
        ],
        'entity_name': 'entity',
        'entity_col': 'entity',
        'value_col': 'values',
        'expected': pd.DataFrame(
            {
                'entity': ['b', 'a'],
                'values': [2, 3],
                'timestamp': [datetime.fromtimestamp(0), datetime.fromtimestamp(0)]
            }
        ),
        'ts_col': "timestamp"
    },
}

transform = {
    'Simple':{
        'columns': ['entity', 'values', 'timestamp'],
        'values': [['a', 1, 0]],
    },
    'Simple2':{
        'columns': ['entity', 'values', 'timestamp'],
        'values': [['a', 1, 0]],
    },
    'GroupBy': {
        'columns': ['entity', 'values', 'timestamp'],
        'values': [
            ['a', 1, 0],
            ['a', 10, 0],
        ],
    },
    'Complex': {
        'columns': ['entity', 'values1', 'values2', 'timestamp'],
        'values': [
            ['a', 1, 2, 0],
            ['a', 10, 2, 0]
        ],
    }
}

training_set = {
    'Empty': {
        'features': [
            {
                'columns': ['entity', 'value'],
                'values': [],
                'ts_col': ""
            },

        ],
        'label': {
            'columns': ['entity', 'value'],
            'values': [],
            'ts_col': ""
        },
        'entity': 'user',
        'entity_loc': 'entity',
        'expected': []
    },
    'Simple': {
        'features': [
            {
                'columns': ['entity', 'value'],
                'values': [
                    ['a', 'one'],
                    ['b', 'two'],
                    ['c', 'three'],
                ],
                'ts_col': ""
            },
            {
                'columns': ['entity', 'value'],
                'values': [
                    ['a', 1],
                    ['b', 2],
                    ['c', 3],
                ],
                'ts_col': ""
            }
        ],
        'label': {
            'columns': ['entity', 'value'],
            'values': [
                ['a', True],
                ['b', False],
                ['c', True],
            ],
            'ts_col': ""
        },
        'entity': 'user',
        'entity_loc': 'entity',
        'expected': [
            [['one', 1], True],
            [['two', 2], False],
            [['three', 3], True]
        ],
    },
    'Complex': {
        'features': [
            {
                'columns': ['entity', 'value'],
                'values': [
                    ['a', 1],
                    ['b', 2],
                    ['c', 3],
                    ['a', 4],
                ],
                'ts_col': ""
            },
            {
                'columns': ['entity', 'value', "ts"],
                'values': [
                    ['a', "doesnt exist", datetime.fromtimestamp(11)],
                ],
                'ts_col': "ts"
            },
            {
                'columns': ['entity', 'value', "ts"],
                'values': [
                    ['c', "real value first", datetime.fromtimestamp(5)],
                    ['c', "real value second", datetime.fromtimestamp(5)],
                    ['c', "overwritten", datetime.fromtimestamp(4)],
                ],
                'ts_col': "ts"
            },
            {
                'columns': ['entity', 'value', "ts"],
                'values': [
                    ['b', "first", datetime.fromtimestamp(3)],
                    ['b', "second", datetime.fromtimestamp(4)],
                    ['b', "third", datetime.fromtimestamp(8)],
                ],
                'ts_col': "ts"
            },
            {
                'columns': ['entity', 'value', "ts"],
                'values': [
                ],
                'ts_col': "ts"
            }
        ],
        'label': {
            'columns': ['entity', 'value', 'ts'],
            'values': [
                ['a', 1, datetime.fromtimestamp(10)],
                ['b', 9, datetime.fromtimestamp(3)],
                ['b', 5, datetime.fromtimestamp(5)],
                ['c', 3, datetime.fromtimestamp(7)],
            ],
            'ts_col': "ts"
        },
        'entity': 'user',
        'entity_loc': 'entity',
        'expected': [
            [4, np.NAN, np.NAN, np.NAN, np.NAN, 1],
            [2, np.NAN, np.NAN, "first", np.NAN, 9],
            [2, np.NAN, np.NAN, "second", np.NAN, 5],
            [3, np.NAN, "real value second", np.NAN, np.NAN, 3]
        ],
    },
}
