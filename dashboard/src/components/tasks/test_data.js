export const taskRunsResponse = [
  {
    task: {
      id: 0,
      name: 'Test_job',
      type: 'ResourceCreation',
      target: {
        name: 'transaction',
        variant: 'default',
      },
      targetType: 'NameVariant',
      dateCreated: '2024-02-23T01:25:39.552616793Z',
    },
    taskRun: {
      runId: 0,
      taskId: 0,
      name: 'Test_job',
      trigger: {
        triggerName: 'Apply',
      },
      triggerType: 'OneOffTrigger',
      status: 'SUCCESS',
      startTime: '2024-02-23T01:20:39.552620918Z',
      endTime: '2024-02-23T01:35:39.552620918Z',
      logs: ['log 1', 'log 2'],
      error: 'No errors for now.',
    },
  },
  {
    task: {
      id: 0,
      name: 'Test_job',
      type: 'ResourceCreation',
      target: {
        name: 'transaction',
        variant: 'default',
      },
      targetType: 'NameVariant',
      dateCreated: '2024-02-23T01:25:39.552616793Z',
    },
    taskRun: {
      runId: 1,
      taskId: 0,
      name: 'Test_job',
      trigger: {
        triggerName: 'Apply',
      },
      triggerType: 'OneOffTrigger',
      status: 'SUCCESS',
      startTime: '2024-02-23T01:20:39.552621877Z',
      endTime: '2024-02-23T01:35:39.552621877Z',
      logs: ['log 1', 'log 2'],
      error: 'No errors for now.',
    },
  },
];

export const taskCardDetailsResponse = {
  taskRun: {
    runId: 1,
    taskId: 1,
    name: 'Sandbox_Test',
    trigger: {
      triggerName: 'Apply',
    },
    triggerType: 'OneOffTrigger',
    status: 'PENDING',
    startTime: '2024-02-22T23:59:56.831528302Z',
    endTime: '2024-02-23T00:14:56.831528302Z',
    logs: ['log 1', 'log 2'],
    error: 'No errors for now.',
  },
  otherRuns: [
    {
      runId: 2,
      taskId: 1,
      name: 'Sandbox_Test',
      trigger: {
        triggerName: 'Apply',
      },
      triggerType: 'OneOffTrigger',
      status: 'PENDING',
      startTime: '2024-02-22T23:59:56.831528927Z',
      endTime: '2024-02-23T00:14:56.831528927Z',
      logs: ['log 1', 'log 2'],
      error: 'No errors for now.',
    },
  ],
};
