import { ThemeProvider } from '@mui/material/styles';
import { configureStore, createSlice } from '@reduxjs/toolkit';
import { cleanup, fireEvent, render } from '@testing-library/react';
import 'jest-canvas-mock';
import React from 'react';
import ReduxWrapper from '../../../components/redux/wrapper/ReduxWrapper';
import TEST_THEME from '../../../styles/theme';
import VariantControl from './VariantControl';

describe('Variant Control Tests', () => {
  const SELECT_ID = 'variantControlSelectId';
  const LIST_NODE = 'LI';

  //unordered list, the postfix represents the expected order
  // by most recent: 0->4
  const RESOURCE_MOCK = {
    'all-variants': [
      'keen_wiles_3',
      'altruistic_hoover_1',
      'ecstatic_heyrovsky_2',
      'dedicated_poitras_0',
      'vigorous_heisenberg_4',
    ],
    variants: {
      altruistic_hoover_1: {
        variant: 'altruistic_hoover_1',
        created: '2020-11-01T23:08:25.301884462Z',
      },
      dedicated_poitras_0: {
        variant: 'dedicated_poitras_0',
        created: '2023-10-31T20:23:30.728769926Z',
      },
      ecstatic_heyrovsky_2: {
        variant: 'ecstatic_heyrovsky_2',
        created: '2015-11-02T16:46:46.601499508Z',
      },
      keen_wiles_3: {
        variant: '4_keen_wiles_3',
        created: '2015-11-02T15:03:03.210467718Z',
      },
      vigorous_heisenberg_4: {
        variant: '5_vigorous_heisenberg_4',
        created: '2010-11-03T15:24:58.705404755Z',
      },
    },
  };

  const mockHandler = jest.fn();

  const getTestBody = (
    variant = RESOURCE_MOCK['all-variants'][0],
    variantListProp = RESOURCE_MOCK['all-variants'],
    resources = RESOURCE_MOCK,
    handleVariantChange = mockHandler
  ) => {
    const slice = createSlice({
      name: 'testSlice',
      initialState: {},
    });
    const store = configureStore({
      reducer: slice.reducer,
    });
    return (
      <>
        <ReduxWrapper store={store}>
          <ThemeProvider theme={TEST_THEME}>
            <VariantControl
              variant={variant}
              variantListProp={variantListProp}
              resources={resources}
              handleVariantChange={handleVariantChange}
            />
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

  test('Basic render displays all options', async () => {
    //given:
    const helper = render(getTestBody());

    //when:
    const dropdown = helper.getByTestId(SELECT_ID);
    fireEvent.mouseDown(helper.getByRole('button'));

    //then:
    expect(dropdown.nodeName).toBeDefined();
    expect(dropdown.textContent).toContain(RESOURCE_MOCK['all-variants'][0]);

    RESOURCE_MOCK['all-variants'].map((vr) => {
      const foundItem = document.body.querySelector(`li[data-value="${vr}"]`);
      expect(foundItem.nodeName).toBe(LIST_NODE);
    });
  });

  test('The selection list is rendered in order by the `created` property', async () => {
    //given:
    const helper = render(getTestBody());

    //when:
    const dropdown = helper.getByTestId(SELECT_ID);
    fireEvent.mouseDown(helper.getByRole('button'));

    //then:
    expect(dropdown.nodeName).toBeDefined();
    expect(dropdown.textContent).toContain(RESOURCE_MOCK['all-variants'][0]);

    //all variants render in the expected order
    const listItems = document.body.querySelectorAll('li');
    expect(listItems?.length).toBe(RESOURCE_MOCK['all-variants'].length);

    for (let i = 0; i < listItems.length; i++) {
      let txt = listItems[i].textContent;
      let expectedPostFix = txt[txt.length - 1];
      expect(expectedPostFix).toBe(`${i}`);
    }
  });

  test('Selecting an item fires off the handleChange handler', async () => {
    //given:
    const helper = render(getTestBody());
    const variantOption = RESOURCE_MOCK['all-variants'][1];

    //when:
    helper.getByTestId(SELECT_ID);
    fireEvent.mouseDown(helper.getByRole('button'));

    const item = helper.getByText(variantOption);
    fireEvent.click(item);

    //then:
    expect(mockHandler).toHaveBeenCalledTimes(1);
    expect(mockHandler).toHaveBeenCalledWith(variantOption);
  });

  test('An ordering error still renders the passed in prop list', async () => {
    //given: a resource mock with an intentional wrong variants prop
    console.error = jest.fn();
    const MOCK_COPY = { ...RESOURCE_MOCK, variants: ['wrong data type!'] };
    const variant = MOCK_COPY['all-variants'][0];
    const allVariants = MOCK_COPY['all-variants'];
    const helper = render(getTestBody(variant, allVariants, MOCK_COPY));

    //when:
    const dropdown = helper.getByTestId(SELECT_ID);
    fireEvent.mouseDown(helper.getByRole('button'));

    //then: an error was called but the list renders OK
    expect(console.error).toHaveBeenCalledTimes(1);

    expect(dropdown.nodeName).toBeDefined();
    expect(dropdown.textContent).toContain(variant);
    allVariants.map((vr) => {
      const foundItem = document.body.querySelector(`li[data-value="${vr}"]`);
      expect(foundItem.nodeName).toBe(LIST_NODE);
    });
  });
});
