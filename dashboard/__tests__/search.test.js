import { ThemeProvider } from '@mui/material/styles';
import { cleanup, fireEvent, render } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import 'jest-canvas-mock';
import React from 'react';
import SearchBar from '../src/components/search/SearchBar';
import TEST_THEME from '../src/styles/theme';

const userRouterMock = {
  push: jest.fn(),
};

jest.mock('next/router', () => ({
  useRouter: () => userRouterMock,
}));

describe('Search Input Tests', () => {
  const SEARCH_INPUT_ID = 'searchInputId';
  const SEARCH_URI = '/query?q=';
  const USER_EVENT_ENTER = '{enter}';
  const USER_EVENT_DELETE = '{backspace}';

  const getTestBody = () => {
    return (
      <>
        <ThemeProvider theme={TEST_THEME}>
          <SearchBar homePage />
        </ThemeProvider>
      </>
    );
  };

  beforeEach(() => {
    jest.resetAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  test('Issue-762: A basic search string invokes the router.', async () => {
    //given:
    const searchText = 'Anthony';
    const helper = render(getTestBody());

    //when: the user types and hits enter
    const searchField = helper.getByTestId(SEARCH_INPUT_ID);
    await userEvent.type(searchField, `${searchText}${USER_EVENT_ENTER}`);

    //then: a router call is invoked
    expect(userRouterMock.push).toHaveBeenCalledTimes(1);
    expect(userRouterMock.push).toHaveBeenCalledWith(
      `${SEARCH_URI}${searchText}`
    );
  });

  test('Issue-762: The user can incrementally delete the search input', async () => {
    //given:
    const searchText = 'Anthony';
    const helper = render(getTestBody());

    //and: an initial search term is entered
    const searchField = helper.getByTestId(SEARCH_INPUT_ID);
    fireEvent.change(searchField, { target: { value: searchText } });
    expect(searchField.value).toBe(searchText);

    //when: the user deletes all the characters one by one, then presses enter
    await userEvent.type(
      searchField,
      USER_EVENT_DELETE.repeat(searchText.length)
    );
    await userEvent.type(searchField, USER_EVENT_ENTER);

    //then: the field is empty, and the enter invoked no router.push calls
    expect(searchField.value).toBe('');
    expect(userRouterMock.push).toHaveBeenCalledTimes(0);
  });

  test.each`
    SearchInputParam     | InvokeParam
    ${'noSpaceSearch'}   | ${1}
    ${'spaces between'}  | ${1}
    ${'spaces after   '} | ${1}
    ${'  spaces before'} | ${1}
    ${''}                | ${0}
    ${' '}               | ${0}
  `(
    `Issue-762: The search entry "$SearchInputParam" invokes the router: $InvokeParam times`,
    async ({ SearchInputParam, InvokeParam }) => {
      //given:
      const helper = render(getTestBody());

      //when: the user initiates a search
      const searchField = helper.getByTestId(SEARCH_INPUT_ID);
      await userEvent.type(
        searchField,
        `${SearchInputParam}${USER_EVENT_ENTER}`
      );

      //then:
      expect(userRouterMock.push).toHaveBeenCalledTimes(InvokeParam);
      if (InvokeParam) {
        expect(userRouterMock.push).toHaveBeenCalledWith(
          `${SEARCH_URI}${SearchInputParam.trim()}`
        );
      }
    }
  );
});
