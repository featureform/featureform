import {
  cleanup,
  fireEvent,
  render,
  waitForElementToBeRemoved,
} from '@testing-library/react';
import 'jest-canvas-mock';
import React from 'react';
import SourceDialog from './SourceDialog';
import testData from './transactions.test.json';

describe('Source Table Dialog Tests', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  const H2_NODE = 'H2';
  const TH_NODE = 'TH';
  const DIV_NODE = 'DIV';
  const P_NODE = 'P';
  const OPEN_BTN_ID = 'sourceTableOpenId';
  const CLOSE_BTN_ID = 'sourceTableCloseId';
  const ERROR_MSG_ID = 'errorMessageId';
  const TITLE_ID = 'sourceTableTitleId';
  const DEFAULT_NAME = 'Test Table';
  const DEFAULT_VARIANT = 'default';
  const ERROR_MSG = 'Error 500 - Something went wrong';

  const apiDataMock = {
    fetchSourceModalData: jest.fn().mockResolvedValue(testData),
  };

  const apiEmptyMock = {
    fetchSourceModalData: jest.fn().mockResolvedValue({
      columns: [],
      rows: [],
    }),
  };

  const apiErrorMock = {
    fetchSourceModalData: jest.fn().mockResolvedValue(ERROR_MSG),
  };

  const getTestBody = (
    apiParam = apiDataMock,
    name = DEFAULT_NAME,
    variant = DEFAULT_VARIANT
  ) => {
    const testBody = (
      <SourceDialog api={apiParam} sourceName={name} sourceVariant={variant} />
    );
    return { testBody, apiParam, name };
  };

  test('Dialog renders OK with an empty col/row response.', async () => {
    //given:
    const { testBody, apiParam, name } = getTestBody(apiEmptyMock);
    const helper = render(testBody);

    //when: the user clicks open
    fireEvent.click(helper.getByTestId(OPEN_BTN_ID));
    const foundName = await helper.findByTestId(TITLE_ID);

    //then:
    expect(apiParam.fetchSourceModalData).toHaveBeenCalledTimes(1);
    expect(apiParam.fetchSourceModalData).toHaveBeenCalledWith(
      name,
      DEFAULT_VARIANT
    );
    expect(foundName.textContent).toBe(DEFAULT_NAME.toUpperCase());
    expect(foundName.nodeName).toBe(H2_NODE);
  });

  test('The dialog table renders all available columns', async () => {
    //given:
    const { testBody } = getTestBody(apiDataMock);
    const helper = render(testBody);

    //when: the user clicks open
    fireEvent.click(helper.getByTestId(OPEN_BTN_ID));
    await helper.findByTestId(TITLE_ID);

    //then: each column is rended on the table
    testData.columns.map((column) => {
      const foundCol = helper.getByText(column);
      expect(foundCol.nodeName).toBe(TH_NODE);
    });
  });

  test('The dialog table renders the initial data row', async () => {
    //given:
    const { testBody } = getTestBody(apiDataMock);
    const helper = render(testBody);

    //when: the user clicks open
    fireEvent.click(helper.getByTestId(OPEN_BTN_ID));
    await helper.findByTestId(TITLE_ID);

    //then: the data row's values are rendered OK
    testData.rows[0].map((row) => {
      const foundRow = helper.getAllByText(row);
      expect(foundRow.length).toBeGreaterThan(0);
      expect(foundRow[0].nodeName).toBe(P_NODE);
    });
  });

  test('Fetch errors are displayed to the user', async () => {
    //given:
    const { testBody, apiParam, name } = getTestBody(apiErrorMock);
    const helper = render(testBody);

    //when: the user clicks open
    fireEvent.click(helper.getByTestId(OPEN_BTN_ID));
    const foundError = await helper.findByTestId(ERROR_MSG_ID);

    //then:
    expect(apiParam.fetchSourceModalData).toHaveBeenCalledTimes(1);
    expect(apiParam.fetchSourceModalData).toHaveBeenCalledWith(
      name,
      DEFAULT_VARIANT
    );
    expect(foundError.textContent).toBe(ERROR_MSG);
    expect(foundError.nodeName).toBe(DIV_NODE);
  });

  test('The dialog closes on user click', async () => {
    //given:
    const { testBody } = getTestBody();
    const helper = render(testBody);
    fireEvent.click(helper.getByTestId(OPEN_BTN_ID));

    //and: verify the modal opens
    await helper.findByTestId(TITLE_ID);

    //when:
    fireEvent.click(helper.getByTestId(CLOSE_BTN_ID));
    let titleQuery = helper.queryByTestId(TITLE_ID);
    await waitForElementToBeRemoved(() => helper.queryByTestId(TITLE_ID));

    //then:
    expect(titleQuery).not.toBeInTheDocument();
  });
});
