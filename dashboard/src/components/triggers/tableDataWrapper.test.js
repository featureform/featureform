import { ThemeProvider } from '@mui/material/styles';
import { cleanup, fireEvent, render } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import 'jest-canvas-mock';
import React from 'react';
import TEST_THEME from '../../styles/theme';
import TableDataWrapper from './tableDataWrapper';
import { triggerDetail, triggerResponse } from './test_data';

const dataAPIMock = {
  getTriggers: jest.fn().mockResolvedValue(triggerResponse),
  postTrigger: jest.fn(),
  getTriggerDetails: jest.fn().mockResolvedValue(triggerDetail),
};

jest.mock('../../hooks/dataAPI', () => ({
  useDataAPI: () => {
    return dataAPIMock;
  },
}));

describe('Trigger table data wrapper tests', () => {
  const DIV_NODE = 'DIV';
  const P_NODE = 'P';
  const SEARCH_INPUT_ID = 'searcInputId';
  const NEW_TRIGGER_BTN_ID = 'newTriggerId';
  const NEW_TRIGGER_SAVE_ID = 'saveNewTriggerId';
  const CREATE_TITLE = 'Create New Trigger';
  const USER_EVENT_ENTER = '{enter}';
  const USER_EVENT_DELETE = '{backspace}';
  const VALIDATION_ERROR = 'Enter a value.';
  const TRIGGER_NAME_INPUT_ID = 'triggerNameInputId';
  const TRIGGER_SCHEDULE_INPUT_ID = 'triggerScheduleInputId';
  const DETAIL_TYPE_ID = 'detailTypeId';
  const DETAIL_SCHEDULE_ID = 'detailScheduleId';
  const DETAIL_OWNER_ID = 'detailOwnerId';
  const DELETE_TRIGGER_BTN_ID = 'deleteTriggerBtnId';

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

    await helper.findByTestId(NEW_TRIGGER_BTN_ID);

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

    await helper.findByTestId(NEW_TRIGGER_BTN_ID);

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

    await helper.findByTestId(NEW_TRIGGER_BTN_ID);

    //expect: validation kicks in
    expect(foundTitle).toBeDefined();
    expect(foundErrors.length).toBe(2); //2 input fields
    expect(dataAPIMock.getTriggers).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.getTriggers).toHaveBeenCalledWith('');
  });

  test('New trigger with valid inputs will post values', async () => {
    //given:
    const helper = render(getTestBody());
    const inputValue = 'text input';

    //and: the user opens the new trigger modal and enters values
    const newTriggerBtn = await helper.findByTestId(NEW_TRIGGER_BTN_ID);
    fireEvent.click(newTriggerBtn);

    const foundNameInput = helper.getByTestId(TRIGGER_NAME_INPUT_ID);
    await userEvent.type(foundNameInput, inputValue);
    expect(foundNameInput.value).toBe(inputValue);

    const foundScheduleInput = helper.getByTestId(TRIGGER_SCHEDULE_INPUT_ID);
    await userEvent.type(foundScheduleInput, inputValue);
    expect(foundScheduleInput.value).toBe(inputValue);

    // when: they click save
    const newTriggerSaveBtn = helper.getByTestId(NEW_TRIGGER_SAVE_ID);
    fireEvent.click(newTriggerSaveBtn);

    await helper.findByTestId(NEW_TRIGGER_BTN_ID);

    //expect: the request is fired off
    expect(dataAPIMock.getTriggers).toHaveBeenCalledTimes(2);
    expect(dataAPIMock.getTriggers).toHaveBeenCalledWith('');
    expect(dataAPIMock.postTrigger).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.postTrigger).toHaveBeenCalledWith({
      triggerName: inputValue,
      schedule: inputValue,
    });
  });

  test('The trigger detail renders OK', async () => {
    //given:
    const helper = render(getTestBody());
    const test_data = { ...triggerResponse };

    //and: details is invoked
    const foundRecord1 = await helper.findByText(test_data[0].name);
    fireEvent.click(foundRecord1);

    //when: the details load, and delete is disabled
    const foundType = await helper.findByTestId(DETAIL_TYPE_ID);
    const foundSchedule = await helper.findByTestId(DETAIL_SCHEDULE_ID);
    const foundOwner = await helper.findByTestId(DETAIL_OWNER_ID);
    const disabledDelete = helper.getByTestId(DELETE_TRIGGER_BTN_ID);

    //expect:
    expect(dataAPIMock.getTriggers).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.getTriggers).toHaveBeenCalledWith('');
    expect(dataAPIMock.getTriggerDetails).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.getTriggerDetails).toHaveBeenCalledWith(test_data[0].id);
    expect(foundType.nodeName).toBe(P_NODE);
    expect(foundSchedule.nodeName).toBe(P_NODE);
    expect(foundOwner.nodeName).toBe(P_NODE);
    expect(disabledDelete.textContent).toBe('Delete Trigger');
    expect(disabledDelete).toBeDisabled();
  });
});
