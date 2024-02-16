import { ThemeProvider } from '@mui/material/styles';
import { cleanup, fireEvent, render } from '@testing-library/react';
import 'jest-canvas-mock';
import React from 'react';
import TEST_THEME from '../../styles/theme';
import TaskRunCard from './taskRunCard';
import { taskCardDetailsResponse, taskRunsResponse } from './test_data';

const dataAPIMock = {
  getTaskRuns: jest.fn().mockResolvedValue(taskRunsResponse),
  getTaskRunDetails: jest.fn().mockResolvedValue(taskCardDetailsResponse),
};

jest.mock('../../hooks/dataAPI', () => ({
  useDataAPI: () => {
    return dataAPIMock;
  },
}));

describe('Task run card detail tests', () => {
  const REFRESH_ICON_ID = 'taskRunRrefreshIcon';
  const getTestBody = (taskRunId) => {
    return (
      <>
        <ThemeProvider theme={TEST_THEME}>
          <TaskRunCard handleClose={jest.fn()} searchId={taskRunId} />
        </ThemeProvider>
      </>
    );
  };

  beforeEach(() => {});

  afterEach(() => {
    jest.resetAllMocks();
    jest.restoreAllMocks();
    cleanup();
  });

  test('Basic task card detail render', async () => {
    //given:
    jest.useFakeTimers();
    const taskRunId = taskCardDetailsResponse.id;
    const helper = render(getTestBody(taskRunId));

    const foundRefreshBtn = await helper.findByTestId(REFRESH_ICON_ID);
    fireEvent.click(foundRefreshBtn);
    jest.advanceTimersByTime(1500);
    await helper.findByTestId(REFRESH_ICON_ID);

    //expect:
    expect(dataAPIMock.getTaskRunDetails).toHaveBeenCalledTimes(2);
    expect(dataAPIMock.getTaskRunDetails).toHaveBeenCalledWith(
      taskCardDetailsResponse.id
    );
  });
});
