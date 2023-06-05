import { ThemeProvider } from '@material-ui/core';
import { cleanup, render } from '@testing-library/react';
import 'jest-canvas-mock';
import React from 'react';
import TopBar from '../src/components/topbar/TopBar';
import TEST_THEME from '../src/styles/theme';

const mockVersion = 'v0.8.1';
const apiMock = {
  fetchVersionMap: () => {
    return Promise.resolve({ data: { version: mockVersion } });
  },
};

jest.mock('next/router', () => ({
  useRouter: () => {
    return {
      push: jest.fn(),
      query: '',
    };
  },
}));

describe('TopBar version tests', () => {
  const VERSION_ID = 'versionPropId';

  const getTestBody = (api = apiMock) => {
    return (
      <>
        <ThemeProvider theme={TEST_THEME}>
          <TopBar api={api} />
        </ThemeProvider>
      </>
    );
  };

  beforeEach(() => {
    console.warn = jest.fn();
    jest.resetAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  test('Issue-812: The Top Bar component displays the correct version data', async () => {
    //given:
    const helper = render(getTestBody());

    // when: we find the version elem
    const versionField = await helper.findByTestId(VERSION_ID);

    // then: the correct value displays
    expect(versionField).toBeDefined();
    expect(versionField.nodeName).toBe('SPAN');
    expect(versionField.textContent).toBe(`Version: ${mockVersion}`);
  });
});
