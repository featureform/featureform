import { ThemeProvider } from '@mui/material/styles';
import { cleanup, fireEvent, render } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import 'jest-canvas-mock';
import React from 'react';
import TEST_THEME from '../../styles/theme';
import TableDataWrapper from './tableDataWrapper';
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

describe('Task table data wrapper tests', () => {
  const ALL_ID = 'allId';
  const SPAN_NODE = 'SPAN';
  const FILTER_ALL = 'All';
  const FILTER_ACTIVE = 'Active';
  const FILTER_COMPLETE = 'Complete';
  const SEARCH_INPUT_ID = 'searcInputId';
  const USER_EVENT_ENTER = '{enter}';
  const REFRESH_ICON_ID = 'refreshIcon';
  const DEFAULT_PARAMS = { searchText: '', sortBy: '', status: 'ALL' };

  const getTestBody = () => {
    return (
      <>
        <ThemeProvider theme={TEST_THEME}>
          <TableDataWrapper />
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

  test('Basic main table render with 2 task run records', async () => {
    //given:
    const helper = render(getTestBody());

    //when:
    const foundFilterAll = await helper.findByText(FILTER_ALL);
    const foundAllChip = helper.getByTestId(ALL_ID);
    helper.getByText(FILTER_ACTIVE);
    helper.getByText(FILTER_COMPLETE);

    //expect:
    expect(foundFilterAll.nodeName).toBe(SPAN_NODE);
    expect(dataAPIMock.getTaskRuns).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.getTaskRuns).toHaveBeenCalledWith(DEFAULT_PARAMS);
    expect(dataAPIMock.getTaskRunDetails).not.toHaveBeenCalled();
    expect(foundAllChip.textContent).toBe(taskRunsResponse.length.toString());
  });

  test('Typing into the search box and hitting enter fires off a request', async () => {
    //given:
    const searchTerm = 'my search words';
    const helper = render(getTestBody());

    //when: the user types and hits enter
    const foundSearchInput = await helper.findByTestId(SEARCH_INPUT_ID);
    await userEvent.type(foundSearchInput, `${searchTerm}${USER_EVENT_ENTER}`);
    await helper.findByTestId(SEARCH_INPUT_ID);

    //then: the api is invoked
    expect(dataAPIMock.getTaskRuns).toHaveBeenCalledTimes(2);
    expect(dataAPIMock.getTaskRuns).toHaveBeenCalledWith(
      expect.objectContaining({ ...DEFAULT_PARAMS, searchText: searchTerm })
    );
  });

  test('Clicking table refresh fires off a search', async () => {
    //given:
    const helper = render(getTestBody());
    jest.useFakeTimers();

    // when:
    const foundRefreshBtn = await helper.findByTestId(REFRESH_ICON_ID);
    fireEvent.click(foundRefreshBtn);
    jest.advanceTimersByTime(1500);
    await helper.findByTestId(REFRESH_ICON_ID);

    //then: the api is invoked twice. on initial load and refresh
    expect(dataAPIMock.getTaskRuns).toHaveBeenCalledTimes(2);
  });
});
