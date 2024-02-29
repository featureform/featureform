import { ThemeProvider } from '@mui/material/styles';
import { cleanup, fireEvent, render } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import 'jest-canvas-mock';
import React from 'react';
import TEST_THEME from '../../styles/theme';
import TableDataWrapper from './tableDataWrapper';
import { triggerResponse } from './test_data';

const dataAPIMock = {
  getTriggers: jest.fn().mockResolvedValue(triggerResponse),
};

jest.mock('../../hooks/dataAPI', () => ({
  useDataAPI: () => {
    return dataAPIMock;
  },
}));

describe('Trigger table data wrapper tests', () => {
  const DIV_NODE = 'DIV';
  const SEARCH_INPUT_ID = 'searcInputId';
  const NEW_TRIGGER_BTN_ID = 'newTriggerId';
  const NEW_TRIGGER_SAVE_ID = 'saveNewTriggerId';
  const CREATE_TITLE = 'Create New Trigger';
  const USER_EVENT_ENTER = '{enter}';
  const USER_EVENT_DELETE = '{backspace}';
  const VALIDATION_ERROR = 'Enter a value.';

  const getTestBody = () => {
    return (
      <>
        <ThemeProvider theme={TEST_THEME}>
          <TableDataWrapper />
        </ThemeProvider>
      </>
    );
  };

  afterEach(() => {
    jest.resetAllMocks();
    cleanup();
  });

  test('Main trigger table renders 2 test records', async () => {
    //given:
    const helper = render(getTestBody());
    const test_data = { ...triggerResponse };

    //when:
    const foundRecord1 = await helper.findByText(test_data[0].name);
    const foundRecord2 = await helper.findByText(test_data[1].name);

    //expect:
    expect(foundRecord1.nodeName).toBe(DIV_NODE);
    expect(foundRecord2.nodeName).toBe(DIV_NODE);
    expect(dataAPIMock.getTriggers).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.getTriggers).toHaveBeenCalledWith('');
  });

  test('Typing into the search box and hitting enter fires off a request', async () => {
    //given:
    const searchTerm = 'trigger search';
    const helper = render(getTestBody());

    //when: the user types and hits enter
    const foundSearchInput = await helper.findByTestId(SEARCH_INPUT_ID);
    await userEvent.type(foundSearchInput, `${searchTerm}${USER_EVENT_ENTER}`);
    await helper.findByTestId(SEARCH_INPUT_ID);

    helper.getByTestId(SEARCH_INPUT_ID);

    //then: the api is invoked
    expect(dataAPIMock.getTriggers).toHaveBeenCalledTimes(2);
    expect(dataAPIMock.getTriggers).toHaveBeenNthCalledWith(2, searchTerm);
  });

  test('Clearing the filter inputs fires off a request', async () => {
    //given:
    const helper = render(getTestBody());
    const searchTerm = 'trigger search';

    // and:
    const foundSearchInput = await helper.findByTestId(SEARCH_INPUT_ID);
    await userEvent.type(foundSearchInput, searchTerm);
    expect(foundSearchInput.value).toBe(searchTerm);

    // when: incrementally delete
    await userEvent.type(
      foundSearchInput,
      USER_EVENT_DELETE.repeat(searchTerm.length)
    );

    //then:
    expect(dataAPIMock.getTriggers).toHaveBeenCalledTimes(2);
    expect(dataAPIMock.getTriggers).toHaveBeenNthCalledWith(2, '');
  });

  test('New trigger modal renders with save validation', async () => {
    //given:
    const helper = render(getTestBody());

    //and: the user opens the new trigger modal
    const newTriggerBtn = await helper.findByTestId(NEW_TRIGGER_BTN_ID);
    fireEvent.click(newTriggerBtn);

    //when: the modal opens, and the user immediately clicks the save btn
    const foundTitle = helper.getByText(CREATE_TITLE);
    const newTriggerSaveBtn = helper.getByTestId(NEW_TRIGGER_SAVE_ID);
    fireEvent.click(newTriggerSaveBtn);
    const foundErrors = helper.getAllByText(VALIDATION_ERROR);

    //expect: validation kicks in
    expect(foundTitle).toBeDefined();
    expect(foundErrors.length).toBe(2); //2 input fields
    expect(dataAPIMock.getTriggers).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.getTriggers).toHaveBeenCalledWith('');
  });
});
