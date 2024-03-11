import { ThemeProvider } from '@mui/material/styles';
import { cleanup, fireEvent, render, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import 'jest-canvas-mock';
import React from 'react';
import TEST_THEME from '../../styles/theme';
import TableDataWrapper from './tableDataWrapper';
import { resourceList, triggerDetail, triggerListResponse } from './test_data';
import {
  CONFIRM_DELETE,
  DELETE_FINAL_WARNING,
  DELETE_WARNING,
  PRE_DELETE,
} from './triggerDetail';

const defaultMock = Object.freeze({
  getTriggers: jest.fn().mockResolvedValue(triggerListResponse),
  postTrigger: jest.fn(),
  getTriggerDetails: jest.fn().mockResolvedValue(triggerDetail),
  deleteTrigger: jest.fn().mockResolvedValue(true),
  searchResources: jest.fn().mockResolvedValue(resourceList),
  addTriggerResource: jest.fn(),
});
let dataAPIMock = {};

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
  const ROW_DELETE_BTN_ = 'triggerDelete-';
  const DELETE_WARNING_ID = 'deleteWarning';
  const DELETE_FINAL_ID = 'deleteFinal';
  const AUTOCOMPLETE_ID = 'addResourceId';

  // this is just a little hack to silence a warning that we'll get until we
  // upgrade to 16.9. See also: https://github.com/facebook/react/pull/14853
  const originalError = console.error;
  beforeAll(() => {
    console.error = (...args) => {
      if (/Warning.*not wrapped in act/.test(args[0])) {
        return;
      }
      originalError.call(console, ...args);
    };
  });

  afterAll(() => {
    console.error = originalError;
  });

  const getTestBody = (mockOverride = { ...defaultMock }) => {
    dataAPIMock = mockOverride;
    return (
      <>
        <ThemeProvider theme={TEST_THEME}>
          <TableDataWrapper />
        </ThemeProvider>
      </>
    );
  };

  afterEach(() => {
    jest.clearAllMocks();
    cleanup();
  });

  test('Main trigger table renders 2 test records', async () => {
    //given:
    const helper = render(getTestBody());
    const test_data = { ...triggerListResponse };

    //when:
    const foundRecord1 = await helper.findByText(test_data[0].name);
    const foundRecord2 = await helper.findByText(test_data[1].name);

    //expect:
    expect(foundRecord1.nodeName).toBe(DIV_NODE);
    expect(foundRecord2.nodeName).toBe(DIV_NODE);
    expect(dataAPIMock.getTriggers).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.getTriggers).toHaveBeenCalledWith('');
  });

  test('Typing into the search box and hitting enter fires off a search request', async () => {
    //given:
    const searchTerm = 'custom search';
    const helper = render(getTestBody());

    //when: the user types and hits enter
    const foundSearchInput = await helper.findByTestId(SEARCH_INPUT_ID);
    await userEvent.type(foundSearchInput, `${searchTerm}${USER_EVENT_ENTER}`);
    await helper.findByTestId(SEARCH_INPUT_ID);

    //then: the api is invoked
    expect(dataAPIMock.getTriggers).toHaveBeenCalledTimes(2);
    expect(dataAPIMock.getTriggers).toHaveBeenNthCalledWith(2, searchTerm);
  });

  test('Clearing the filter inputs fires off a request', async () => {
    //given:
    const helper = render(getTestBody());
    const searchTerm = 'trigger search';

    //and:
    const foundSearchInput = await helper.findByTestId(SEARCH_INPUT_ID);
    await userEvent.type(foundSearchInput, searchTerm);
    expect(foundSearchInput.value).toBe(searchTerm);

    //when: incrementally delete
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

  test('New trigger form with valid inputs will post values', async () => {
    //given:
    const helper = render(getTestBody());
    const inputValue = 'input value';
    const scheduleValue = '* * * * *';

    //and: the user opens the new trigger modal and enters values
    const newTriggerBtn = await helper.findByTestId(NEW_TRIGGER_BTN_ID);
    fireEvent.click(newTriggerBtn);

    //field 1
    const foundNameInput = helper.getByTestId(TRIGGER_NAME_INPUT_ID);
    await userEvent.type(foundNameInput, inputValue);
    expect(foundNameInput.value).toBe(inputValue);

    //field 2
    const foundScheduleInput = helper.getByTestId(TRIGGER_SCHEDULE_INPUT_ID);
    await userEvent.type(foundScheduleInput, scheduleValue);
    expect(foundScheduleInput.value).toBe(scheduleValue);

    //when: they click save
    const newTriggerSaveBtn = helper.getByTestId(NEW_TRIGGER_SAVE_ID);
    fireEvent.click(newTriggerSaveBtn);

    await helper.findByTestId(NEW_TRIGGER_BTN_ID);

    //expect: the request is fired off
    expect(dataAPIMock.getTriggers).toHaveBeenCalledTimes(2);
    expect(dataAPIMock.getTriggers).toHaveBeenCalledWith('');
    expect(dataAPIMock.postTrigger).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.postTrigger).toHaveBeenCalledWith({
      triggerName: inputValue,
      schedule: scheduleValue,
    });
  });

  test('The trigger detail renders OK with resources', async () => {
    //given:
    const helper = render(getTestBody());
    const test_data = { ...triggerDetail };

    //and: details is invoked
    const foundRecord1 = await helper.findByText(test_data.trigger.name);
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
    expect(dataAPIMock.getTriggerDetails).toHaveBeenCalledWith(
      test_data.trigger.id
    );
    expect(foundType.nodeName).toBe(P_NODE);
    expect(foundSchedule.nodeName).toBe(P_NODE);
    expect(foundOwner.nodeName).toBe(P_NODE);
    expect(disabledDelete.textContent).toBe('Delete Trigger');
    expect(disabledDelete).toBeDisabled();
  });

  test('The trigger detail renders OK without resources', async () => {
    //given:
    const test_data = { ...triggerDetail, resources: [] };
    const apiOverride = {
      ...defaultMock,
      getTriggerDetails: jest.fn().mockResolvedValue(test_data),
    };
    const helper = render(getTestBody(apiOverride));

    //and: details is invoked
    const foundRecord1 = await helper.findByText(test_data.trigger.name);
    fireEvent.click(foundRecord1);

    //when: the details load, and delete is disabled
    const foundType = await helper.findByTestId(DETAIL_TYPE_ID);
    const foundSchedule = await helper.findByTestId(DETAIL_SCHEDULE_ID);
    const foundOwner = await helper.findByTestId(DETAIL_OWNER_ID);
    const deleteBtn = helper.getByTestId(DELETE_TRIGGER_BTN_ID);

    //expect:
    expect(dataAPIMock.getTriggers).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.getTriggers).toHaveBeenCalledWith('');
    expect(dataAPIMock.getTriggerDetails).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.getTriggerDetails).toHaveBeenCalledWith(
      test_data.trigger.id
    );
    expect(foundType.nodeName).toBe(P_NODE);
    expect(foundSchedule.nodeName).toBe(P_NODE);
    expect(foundOwner.nodeName).toBe(P_NODE);
    expect(deleteBtn.textContent).toBe('Delete Trigger');
    expect(deleteBtn).toBeEnabled();
  });

  test('Clicking delete on a trigger row (that has resources), displays a warning message', async () => {
    //given:
    const test_data = { ...triggerDetail };
    const helper = render(getTestBody());
    await helper.findByText(test_data.trigger.name);

    // and: the delete row is invoked, but the trigger has resources
    const foundRowDelete = await helper.findByTestId(
      ROW_DELETE_BTN_ + test_data.trigger.id
    );
    fireEvent.click(foundRowDelete);

    //when:
    const deleteBtn = await helper.findByTestId(DELETE_TRIGGER_BTN_ID);
    const foundWarning = await helper.findByTestId(DELETE_WARNING_ID);

    //expect: the delete is pre_cofirmed (but disabled) and a warning displays
    expect(dataAPIMock.getTriggerDetails).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.getTriggerDetails).toHaveBeenCalledWith(
      test_data.trigger.id
    );
    expect(deleteBtn.textContent).toBe(CONFIRM_DELETE);
    expect(foundWarning.textContent).toBe(DELETE_WARNING);
    expect(deleteBtn).toBeDisabled();
  });

  test('Clicking delete on a trigger row (that has zero resources), displays a final warning message', async () => {
    //given:
    const test_data = { ...triggerDetail, resources: [] }; //remove resources
    const apiOverride = {
      ...defaultMock,
      getTriggerDetails: jest.fn().mockResolvedValue(test_data),
    };
    const helper = render(getTestBody(apiOverride));
    await helper.findByText(test_data.trigger.name);

    // and: the delete row is invoked, but the trigger has NO resources
    const foundRowDelete = await helper.findByTestId(
      ROW_DELETE_BTN_ + test_data.trigger.id
    );
    fireEvent.click(foundRowDelete);

    //when:
    const deleteBtn = await helper.findByTestId(DELETE_TRIGGER_BTN_ID);
    const foundWarning = await helper.findByTestId(DELETE_FINAL_ID);

    //expect: the delete is pre_cofirmed (but this time enabled) and a final warning displays
    expect(dataAPIMock.getTriggerDetails).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.getTriggerDetails).toHaveBeenCalledWith(
      test_data.trigger.id
    );
    expect(deleteBtn.textContent).toBe(CONFIRM_DELETE);
    expect(foundWarning.textContent).toBe(DELETE_FINAL_WARNING);
    expect(deleteBtn).toBeEnabled();
  });

  test('Confirming delete twice, sends the trigger delete request', async () => {
    //given:
    const test_data = { ...triggerDetail, resources: [] }; //remove resources
    const apiOverride = {
      ...defaultMock,
      getTriggerDetails: jest.fn().mockResolvedValue(test_data),
    };
    const helper = render(getTestBody(apiOverride));
    await helper.findByText(test_data.trigger.name);

    //and: we open the details window
    const foundRecord1 = await helper.findByText(test_data.trigger.name);
    fireEvent.click(foundRecord1);

    //and: we click delete once
    const deleteBtn = await helper.findByTestId(DELETE_TRIGGER_BTN_ID);
    expect(deleteBtn.textContent).toBe(PRE_DELETE);
    fireEvent.click(deleteBtn);

    //when: a final warning displays, we confirm the delete by clicking again
    expect(deleteBtn.textContent).toBe(CONFIRM_DELETE);
    await helper.findByTestId(DELETE_FINAL_ID);
    fireEvent.click(deleteBtn);

    //expect: the delete goes through
    expect(dataAPIMock.getTriggerDetails).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.getTriggerDetails).toHaveBeenCalledWith(
      test_data.trigger.id
    );
    expect(dataAPIMock.deleteTrigger).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.deleteTrigger).toHaveBeenCalledWith(
      test_data.trigger.id
    );
  });

  test('The trigger detail adds resources when the user clicks', async () => {
    //given:
    const helper = render(getTestBody());
    const test_data = { ...triggerDetail };
    const triggerId = test_data.trigger.id;
    const nameVariant = {
      name: 'ondemand_add_integers',
      variant: '1709838479',
    };

    //and: details is invoked
    const foundRecord1 = await helper.findByText(test_data.trigger.name);
    fireEvent.click(foundRecord1);

    //and: the autocomplete is used
    const autocomplete = helper.getByTestId(AUTOCOMPLETE_ID);
    const input = within(autocomplete).getByRole('combobox');
    autocomplete.focus();
    await userEvent.type(input, nameVariant.name);

    //pause
    await helper.findByTestId(DETAIL_TYPE_ID);

    // when: the first option is clicked
    const option = helper.getAllByRole('option')[0];
    fireEvent.click(option);
    await helper.findByTestId(DETAIL_TYPE_ID);

    //expect: add resources is invoked
    expect(input.value).toContain(nameVariant.name);
    expect(dataAPIMock.addTriggerResource).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.addTriggerResource).toHaveBeenCalledWith(
      triggerId,
      nameVariant.name,
      nameVariant.variant
    );
  });
});
