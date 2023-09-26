import { ThemeProvider } from '@mui/material/styles';
import { configureStore, createSlice } from '@reduxjs/toolkit';
import { cleanup, render } from '@testing-library/react';
import 'jest-canvas-mock';
import React from 'react';
import EntityPage from '../src/components/entitypage/EntityPage';
import {
  convertInputToDate,
  getFormattedSQL,
} from '../src/components/entitypage/EntityPageView';
import ReduxWrapper from '../src/components/redux/wrapper/ReduxWrapper';
import TEST_THEME from '../src/styles/theme';

jest.mock('../src/components/entitypage/EntityPageView', () => {
  const originalModule = jest.requireActual(
    '../src/components/entitypage/EntityPageView'
  );
  return {
    __esModule: true,
    ...originalModule,
    default: function MockView() {
      return <div data-testid='entityPageViewId' />;
    },
  };
});

describe('Entity Page Tests', () => {
  const LOADING_DOTS_ID = 'loadingDotsId';
  const NOT_FOUND = 'notFoundId';
  const apiMock = { fetchEntity: jest.fn() };

  const defaultState = Object.freeze({
    entityPage: { loading: false, failed: false },
    selectedVariant: '',
  });

  const getTestBody = (initialState = {}) => {
    const slice = createSlice({
      name: 'testSlice',
      initialState: initialState,
    });
    const store = configureStore({
      reducer: slice.reducer,
    });

    return (
      <>
        <ReduxWrapper store={store}>
          <ThemeProvider theme={TEST_THEME}>
            <EntityPage api={apiMock} type='sources' entity='myEntity' />
          </ThemeProvider>
        </ReduxWrapper>
      </>
    );
  };

  beforeEach(() => {
    jest.resetAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  test('Issue-762: If the entity page fetch is loading, the loading component displays.', async () => {
    //given:
    const state = {
      ...defaultState,
      entityPage: { loading: true, failed: false },
    };
    const helper = render(getTestBody(state));

    //when:
    const loadingDotsDiv = await helper.findByTestId(LOADING_DOTS_ID);

    //then:
    expect(loadingDotsDiv).toBeDefined();
    expect(loadingDotsDiv.nodeName).toBe('DIV');
    expect(apiMock.fetchEntity).toHaveBeenCalledTimes(1);
  });

  test('Issue-762: If the fetch state fails, load the "404 not found" component', async () => {
    //given:
    const state = {
      ...defaultState,
      entityPage: { failed: true, loading: false },
    };
    const helper = render(getTestBody(state));

    //when:
    const notFoundDiv = await helper.findByTestId(NOT_FOUND);
    const foundFoundElem = helper.getByText('404', { exact: false });

    //then:
    expect(notFoundDiv).toBeDefined();
    expect(foundFoundElem.nodeName).toBe('H1');
    expect(apiMock.fetchEntity).toHaveBeenCalledTimes(1);
  });

  test('Issue-762: The fetch completed, but the returned object is empty, load the "404 not found" component', async () => {
    //given:
    const state = {
      ...defaultState,
      entityPage: { failed: false, loading: false },
    };
    const helper = render(getTestBody(state));

    //when:
    const notFoundDiv = await helper.findByTestId(NOT_FOUND);
    const foundFoundElem = helper.getByText('404', { exact: false });

    //then:
    expect(notFoundDiv).toBeDefined();
    expect(foundFoundElem.nodeName).toBe('H1');
    expect(apiMock.fetchEntity).toHaveBeenCalledTimes(1);
  });

  test('Issue-762: The fetch completed, and the returned object is populated, display the entity view component', async () => {
    //given:
    const foundObj = { name: 'a name', type: 'a type' };
    const state = {
      ...defaultState,
      entityPage: { failed: false, loading: false, resources: foundObj },
    };
    const helper = render(getTestBody(state));

    //when:
    const foundPageMock = await helper.findByTestId('entityPageViewId');

    //then:
    expect(foundPageMock).toBeDefined();
    expect(apiMock.fetchEntity).toHaveBeenCalledTimes(1);
  });

  test('Issue-769: If no resource data is found, display the "404 not found" component', async () => {
    //given: an empty resources response obj
    const state = {
      ...defaultState,
      entityPage: { failed: false, loading: false, resources: {} },
    };
    const helper = render(getTestBody(state));

    //when:
    const notFoundDiv = await helper.findByTestId(NOT_FOUND);
    const foundFoundElem = helper.getByText('404', { exact: false });

    //then:
    expect(notFoundDiv).toBeDefined();
    expect(foundFoundElem.nodeName).toBe('H1');
    expect(apiMock.fetchEntity).toHaveBeenCalledTimes(1);
  });

  test('The sql formatter function correctly formats a valid metadata SQL string', async () => {
    //given: a metadata definition sql string
    let sql =
      'SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{ transactions.tender_shannon }} GROUP BY user_id';
    let expectedSQL =
      'SELECT\n  CustomerID as user_id,\n  avg(TransactionAmount) as avg_transaction_amt\nfrom\n  transactions.tender_shannon\nGROUP BY\n  user_id';

    //when: the function is invoked
    let formattedSql = getFormattedSQL(sql);

    //then:
    expect(formattedSql).toBe(expectedSQL);
  });

  test('When the sql formatter function throws an exception, the original table transformation sql string returns', async () => {
    //given:
    console.error = jest.fn();
    let originalInvalidSQL = 'this is not valid SQL!';

    //when: the function is invoked
    let attemptedFormatSql = getFormattedSQL(originalInvalidSQL);

    //then: the error is handled and the original string is returned as safety
    expect(console.error).toHaveBeenCalledWith(
      'There was an error formatting the sql string'
    );
    expect(console.error).toHaveBeenCalledWith(originalInvalidSQL);
    expect(attemptedFormatSql).toBe(originalInvalidSQL);
  });

  test.each`
    CreatedInputParam                  | ResultParam
    ${'1695751185.068369'}             | ${'1/20/1970, 9:02:31 AM'}
    ${'1695751185'}                    | ${'1/20/1970, 9:02:31 AM'}
    ${'2023-09-23T00:10:33.61933372Z'} | ${'9/22/2023, 7:10:33 PM'}
    ${'2023-12-16T00:00:00'}           | ${'12/16/2023, 12:00:00 AM'}
    ${'Not a number or a date string'} | ${'Invalid Date'}
  `(
    `Issue-211: "convertInputToDate() correctly renders the value("$CreatedInputParam") to ("$ResultParam")`,
    ({ CreatedInputParam, ResultParam }) => {
      //given:
      const result = convertInputToDate(CreatedInputParam);

      //expect:
      expect(result).toEqual(ResultParam);
    }
  );
});
