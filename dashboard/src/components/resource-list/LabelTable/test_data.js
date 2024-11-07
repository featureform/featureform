// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

export const label_test_response = {
  count: 2,
  data: [
    {
      created: '2024-08-21T23:21:04.658230674Z',
      description: '',
      entity: 'user',
      name: 'avg_transactions_OK',
      owner: 'anthony@featureform.com',
      provider: 'OK-redis',
      'data-type': 'float32',
      variant: '2024-08-21t18-21-00',
      status: 'FAILED',
      error:
        'Resource Failed: required dataset is in a failed state\n\u003e\u003e\u003e resource_name: average_user_transaction\n\u003e\u003e\u003e resource_variant: 2024-08-21t18-21-00\n\u003e\u003e\u003e resource_type: TRANSFORMATION',
      location: {
        Entity: 'user_id',
        Source: '',
        TS: '',
        Value: 'avg_transaction_amt',
      },
      source: {
        Name: 'average_user_transaction',
        Variant: '2024-08-21t18-21-00',
      },
      'training-sets': null,
      tags: ['testing'],
      properties: {},
      mode: 'PRECOMPUTED',
      'is-on-demand': false,
      definition: '',
    },
    {
      created: '2024-08-02T21:47:41.628119837Z',
      description: '',
      entity: '',
      name: 'example',
      owner: 'anthony@featureform.com',
      provider: '',
      'data-type': '',
      variant: 'example-2',
      status: 'READY',
      error: '',
      location: {
        query:
          '\ufffd\u0004\ufffd\ufffd\u0000\u0000\u0000\u0000\u0000\u0000\u0000\ufffd\ndill._dill\ufffd\ufffd\u000c_create_code\ufffd\ufffd\ufffd(K\u0003K\u0000K\u0000K\u0003K\u0001KCC\u0004d\u0001S\u0000\ufffdNK\u0002\ufffd\ufffd)\ufffd\u0006client\ufffd\ufffd\u0006params\ufffd\ufffd\u0008entities\ufffd\ufffd\ufffd\ufffd\u000ez_on_demand.py\ufffd\ufffd\u0008someFunc\ufffdK\u0008C\u0002\u0000\u0002\ufffd))t\ufffdR\ufffd.',
      },
      source: {
        Name: '',
        Variant: '',
      },
      'training-sets': null,
      tags: ['v1', 'exam1'],
      properties: {},
      mode: 'CLIENT_COMPUTED',
      'is-on-demand': true,
      definition:
        '@ff.ondemand_feature(name=f"example", variant="example-2")\ndef someFunc(client, params, entities):\n    return 1 + 1\n',
    },
  ],
};
