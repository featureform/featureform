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
