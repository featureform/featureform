export const triggerListResponse = [
  {
    id: '3fea3e98-80b7-4b16-8613-944640e0e869',
    name: 'trigger 1',
    type: 'Schedule Trigger',
    schedule: 'schedule 1',
    detail: 'Some Details',
    resources: [
      {
        resourceId: 'eaaa1fd7-8aef-4d7d-a7b2-1f8f75524105',
        resource: 'avg',
        variant: 'v1',
        lastRun: '2024-02-29T21:21:39.53697351Z',
      },
    ],
  },
  {
    id: 'bbd29330-c843-4c2d-8d15-16019a64a3a8',
    name: 'trigger 2',
    type: 'Schedule Trigger',
    schedule: 'schedule 2',
    detail: 'Some Details',
    resources: [],
  },
];

export const triggerDetail = {
  trigger: {
    id: '3fea3e98-80b7-4b16-8613-944640e0e869',
    name: 'trigger 1',
    type: 'Schedule Trigger',
    schedule: 'schedule 1',
    detail: 'Some Details',
    resources: [
      {
        resourceId: 'eaaa1fd7-8aef-4d7d-a7b2-1f8f75524105',
        resource: 'avg',
        variant: 'v1',
        lastRun: '2024-02-29T21:21:39.53697351Z',
      },
      {
        resourceId: 'f357f83a-fdaf-498c-b1a6-d20c1740152a',
        resource: 'avg',
        variant: 'v2',
        lastRun: '2024-02-29T21:21:39.536974385Z',
      },
    ],
  },
  owner: 'Current User',
  resources: [
    {
      resourceId: 'eaaa1fd7-8aef-4d7d-a7b2-1f8f75524105',
      resource: 'avg',
      variant: 'v1',
      lastRun: '2024-02-29T21:21:39.53697351Z',
    },
    {
      resourceId: 'f357f83a-fdaf-498c-b1a6-d20c1740152a',
      resource: 'avg',
      variant: 'v2',
      lastRun: '2024-02-29T21:21:39.536974385Z',
    },
  ],
};

export const resourceList = [
  {
    Name: 'default_user',
    Variant: '',
    Type: 'USER',
    Tags: null,
  },
  {
    Name: 'ondemand_add_integers',
    Variant: '1709838479',
    Type: 'FEATURE_VARIANT',
    Tags: null,
  },
  {
    Name: 'ondemand_add_integers',
    Variant: '',
    Type: 'FEATURE',
    Tags: null,
  },
  {
    Name: 'postgres-quickstart',
    Variant: '',
    Type: 'PROVIDER',
    Tags: null,
  },
  {
    Name: 'redis-quickstart',
    Variant: '',
    Type: 'PROVIDER',
    Tags: null,
  },
  {
    Name: 'transactions',
    Variant: '2024-03-07t13-08-11',
    Type: 'SOURCE_VARIANT',
    Tags: null,
  },
  {
    Name: 'transactions',
    Variant: '',
    Type: 'SOURCE',
    Tags: null,
  },
  {
    Name: 'average_user_transaction',
    Variant: '2024-03-07t13-08-11',
    Type: 'SOURCE_VARIANT',
    Tags: null,
  },
  {
    Name: 'average_user_transaction',
    Variant: '',
    Type: 'SOURCE',
    Tags: null,
  },
  {
    Name: 'user',
    Variant: '',
    Type: 'ENTITY',
    Tags: null,
  },
  {
    Name: 'avg_transactions',
    Variant: '2024-03-07t13-08-11',
    Type: 'FEATURE_VARIANT',
    Tags: null,
  },
  {
    Name: 'avg_transactions',
    Variant: '',
    Type: 'FEATURE',
    Tags: null,
  },
  {
    Name: 'fraudulent',
    Variant: '2024-03-07t13-08-11',
    Type: 'LABEL_VARIANT',
    Tags: null,
  },
  {
    Name: 'fraudulent',
    Variant: '',
    Type: 'LABEL',
    Tags: null,
  },
  {
    Name: 'fraud_training',
    Variant: '2024-03-07t13-08-11',
    Type: 'TRAINING_SET_VARIANT',
    Tags: null,
  },
  {
    Name: 'fraud_training',
    Variant: '',
    Type: 'TRAINING_SET',
    Tags: null,
  },
  {
    Name: 'transactions',
    Variant: '2024-03-07t13-10-14',
    Type: 'SOURCE_VARIANT',
    Tags: null,
  },
  {
    Name: 'average_user_transaction',
    Variant: '2024-03-07t13-10-14',
    Type: 'SOURCE_VARIANT',
    Tags: null,
  },
  {
    Name: 'avg_transactions',
    Variant: '2024-03-07t13-10-14',
    Type: 'FEATURE_VARIANT',
    Tags: null,
  },
  {
    Name: 'fraudulent',
    Variant: '2024-03-07t13-10-14',
    Type: 'LABEL_VARIANT',
    Tags: null,
  },
];
