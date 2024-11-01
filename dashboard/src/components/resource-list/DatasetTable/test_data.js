// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

export const test_data = {
  count: 2,
  data: [
    {
      name: 'transactions',
      variant: '2024-09-06t21-07-40',
      definition: 'transactions',
      owner: 'anthony@featureform.com',
      description: '',
      provider: 'asdf-postgres',
      created: '2024-09-07T02:07:42.257152631Z',
      status: 'READY',
      table: '',
      'training-sets': null,
      features: null,
      labels: null,
      lastUpdated: '1970-01-01T00:00:00Z',
      schedule: '',
      tags: ['my_data', 'm2', 'haha'],
      properties: {},
      'source-type': 'Primary Table',
      error: '',
      specifications: {},
      inputs: [],
    },
    {
      name: 'average_user_transaction',
      variant: '2024-09-06t21-07-40',
      definition:
        'SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{ transactions.2024-09-06t21-07-40 }} GROUP BY user_id',
      owner: 'anthony@featureform.com',
      description: '',
      provider: 'asdf-postgres',
      created: '2024-09-07T02:07:42.921672215Z',
      status: 'READY',
      table: '',
      'training-sets': null,
      features: null,
      labels: null,
      lastUpdated: '1970-01-01T00:00:00Z',
      schedule: '',
      tags: ['avg', 'users'],
      properties: {},
      'source-type': 'SQL Transformation',
      error: '',
      specifications: {},
      inputs: [
        {
          Name: 'transactions',
          Variant: '2024-09-06t21-07-40',
        },
      ],
    },
  ],
};
