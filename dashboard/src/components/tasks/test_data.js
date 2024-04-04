export const taskRunsResponse = [
  {
    task: {
      id: 10,
      name: 'mytask',
      type: 0,
      target: {
        name: 'fraud_training',
        variant: 'sharp_kalam',
        type: 'TRAINING_SET_VARIANT',
      },
      targetType: 0,
      dateCreated: '2024-04-04T03:37:27.458995Z',
    },
    taskRun: {
      runId: 10,
      taskId: 10,
      name: 'Create Resource fraud_training (sharp_kalam)',
      trigger: {
        triggerName: 'Apply',
      },
      triggerType: 1,
      status: 3,
      startTime: '2024-04-04T03:37:27.45907Z',
      endTime: '2024-04-04T03:37:34.311156Z',
      logs: [
        'Starting Execution...',
        'Starting Training Set Creation...',
        'Fetching Training Set configuration...',
        'Fetching Offline Store...',
        'Waiting for dependencies to complete...',
        'Fetching Job runner...',
        'Starting execution...',
        'Waiting for completion...',
        'Training Set creation complete...',
      ],
      error: '',
      ErrorProto: null,
    },
  },
  {
    task: {
      id: 9,
      name: 'mytask',
      type: 0,
      target: {
        name: 'fraudulent',
        variant: 'sharp_kalam',
        type: 'LABEL_VARIANT',
      },
      targetType: 0,
      dateCreated: '2024-04-04T03:37:27.45785Z',
    },
    taskRun: {
      runId: 9,
      taskId: 9,
      name: 'Create Resource fraudulent (sharp_kalam)',
      trigger: {
        triggerName: 'Apply',
      },
      triggerType: 1,
      status: 3,
      startTime: '2024-04-04T03:37:27.457924Z',
      endTime: '2024-04-04T03:37:29.446822Z',
      logs: [
        'Starting Execution...',
        'Fetching Label details...',
        'Waiting for dependencies to complete...',
        'Fetching Offline Store...',
        'Registering Label from dataset...',
        'Registration complete...',
      ],
      error: '',
      ErrorProto: null,
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
