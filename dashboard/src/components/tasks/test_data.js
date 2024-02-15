export const taskRunsResponse = [
  {
    id: '1',
    taskId: 1,
    name: 'Nicole_Puppers',
    type: 'Source',
    provider: 'Postgres',
    resource: 'per_balance2',
    variant: '',
    status: 'SUCCESS',
    lastRunTime: '2024-02-14T19:16:55.799976227Z',
    triggeredBy: 'On Apply',
  },
  {
    id: '2',
    taskId: 2,
    name: 'Sandbox_Test',
    type: 'Source',
    provider: 'Spark',
    resource: 'avg_transactions',
    variant: '',
    status: 'PENDING',
    lastRunTime: '2024-02-14T19:16:55.799977319Z',
    triggeredBy: 'On Apply',
  },
];

export const taskCardDetailsResponse = {
  id: '5',
  name: 'MySQL Task',
  status: 'SUCCESS',
  logs: 'some log values',
  details: 'some task details',
  otherRuns: [
    {
      id: '15',
      lastRunTime: '2024-02-14T19:16:55.799982119Z',
      status: 'FAILED',
      link: 'Future Link',
    },
  ],
};
