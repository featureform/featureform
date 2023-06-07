import {
  cleanup,
  fireEvent,
  render,
  waitForElementToBeRemoved,
} from '@testing-library/react';
import 'jest-canvas-mock';
import React from 'react';
import SourceTableDialog from './SourceTableDialog';

describe('Source Table Dialog Tests', () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  const H2_NODE = 'H2';
  const OPEN_BTN_ID = 'sourceTableOpenId';
  const CLOSE_BTN_ID = 'sourceTableCloseId';
  const TITLE_ID = 'sourceTableTitleId';
  const DEFAULT_NAME = 'Test Table';

  const getTestBody = (tableName = DEFAULT_NAME) => {
    return <SourceTableDialog tableName={tableName} />;
  };

  test('Dialog renders OK with only title prop argument', () => {
    //given:
    const helper = render(getTestBody());

    //when: the user clicks open
    fireEvent.click(helper.getByTestId(OPEN_BTN_ID));
    const foundName = helper.getByTestId(TITLE_ID);

    //then:
    expect(foundName.textContent).toBe(DEFAULT_NAME);
    expect(foundName.nodeName).toBe(H2_NODE);
  });

  test('Dialog closes on user click', async () => {
    //given:
    const helper = render(getTestBody());
    fireEvent.click(helper.getByTestId(OPEN_BTN_ID));

    //and: verify the modal opens
    helper.getByTestId(TITLE_ID);

    //when:
    fireEvent.click(helper.getByTestId(CLOSE_BTN_ID));
    let titleQuery = helper.queryByTestId(TITLE_ID);
    await waitForElementToBeRemoved(() => helper.queryByTestId(TITLE_ID));

    //then:
    expect(titleQuery).not.toBeInTheDocument();
  });
});
