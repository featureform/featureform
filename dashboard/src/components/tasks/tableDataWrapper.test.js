import { ThemeProvider } from '@mui/material/styles';
import { cleanup, render } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import 'jest-canvas-mock';
import React from 'react';
import TEST_THEME from '../../styles/theme';
import TableDataWrapper from './tableDataWrapper';
import { taskCardDetailsResponse, taskRunsResponse } from './test_data';

//todox: modify result per test?
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
  const ACTIVE_ID = 'activeId';
  const COMPLETE_ID = 'completeId';
  const SPAN_NODE = 'SPAN';
  const FILTER_ALL = 'All';
  const FILTER_ACTIVE = 'Active';
  const FILTER_COMPLETE = 'Complete';
  const SEARCH_INPUT_ID = 'searcInputId';
  const USER_EVENT_ENTER = '{enter}';
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

    //then: the api is invoked
    expect(dataAPIMock.getTaskRuns).toHaveBeenCalledTimes(2);
    expect(dataAPIMock.getTaskRuns).toHaveBeenCalledWith(
      expect.objectContaining({ ...DEFAULT_PARAMS, searchText: searchTerm })
    );
  });
});
