import pandas as pd

features = {
    # 'empty': {
    #     'columns': ['entity', 'values'],
    #     'values': [],
    #     'entity': 'entity',
    #     'value_col': 'values',
    #     'expected':
    #         {
    #             'entity': [],
    #             'values': []
    #         }
    # },
    'NoOverlap': {
        'columns': ['entity', 'values'],
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
        'columns': ['entity', 'values'],
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

