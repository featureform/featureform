import {
  cleanup,
  fireEvent,
  render,
  waitForElementToBeRemoved,
} from '@testing-library/react';
import 'jest-canvas-mock';
import React from 'react';
import ErrorModal, { ERROR_MSG_MAX } from './ErrorModal';

describe('Error modal tests', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  const P_NODE = 'P';
  const BTN_NODE = 'BUTTON';
  const OPEN_BTN_ID = 'openErrorModalId';
  const CLOSE_BTN_ID = 'errorModalCloseId';
  const ERROR_MSG_ID = 'errorMessageId';
  const TITLE_ID = 'errorModalTitleId';
  const FULL_TXT_ID = 'fullTextContent';

  const getTestBody = (errorTxt = '', buttonTxt = 'Show More') => {
    const testBody = (
      <>
        <ErrorModal errorTxt={errorTxt} buttonTxt={buttonTxt} />
      </>
    );
    return { testBody };
  };

  test('An error message shorter than MAX renders OK, without a show more button.', async () => {
    //given:
    const errorMsg = 'Something went wrong';
    const { testBody } = getTestBody(errorMsg);
    const helper = render(testBody);

    //when:
    const foundError = helper.getByTestId(ERROR_MSG_ID);
    const foundButton = helper.queryByTestId(OPEN_BTN_ID);

    //then:
    expect(foundError.nodeName).toBe(P_NODE);
    expect(foundError.textContent).toBe(errorMsg);
    expect(foundButton).toBeNull();
  });

  test('An error message greater than the MAX, will render the show more button.', () => {
    //given:
    const errorMsg = '#'.repeat(ERROR_MSG_MAX + 1);
    const { testBody } = getTestBody(errorMsg);
    const helper = render(testBody);

    //when:
    const foundError = helper.getByText(errorMsg.substring(0, ERROR_MSG_MAX), {
      exact: false,
    });
    const foundButton = helper.getByTestId(OPEN_BTN_ID);

    //then:
    expect(foundError.nodeName).toBe(P_NODE);
    expect(foundButton.nodeName).toBe(BTN_NODE);
  });

  test('The show more button displays the full error message', async () => {
    //given:
    const errorMsg = '#'.repeat(ERROR_MSG_MAX + ERROR_MSG_MAX);
    const { testBody } = getTestBody(errorMsg);
    const helper = render(testBody);

    //and: show more displays, the user clicks open
    fireEvent.click(helper.getByTestId(OPEN_BTN_ID));
    await helper.findByTestId(TITLE_ID);

    //when:
    const foundFullError = helper.getByTestId(FULL_TXT_ID);

    //then: the full error is found
    expect(foundFullError.textContent).toBe(errorMsg);
  });

  test('The full error dialog closes on user click', async () => {
    //given:
    const errorMsg = '#'.repeat(ERROR_MSG_MAX + ERROR_MSG_MAX);
    const { testBody } = getTestBody(errorMsg);
    const helper = render(testBody);

    //and: show more displays, the user clicks open
    fireEvent.click(helper.getByTestId(OPEN_BTN_ID));
    await helper.findByTestId(TITLE_ID);

    //when:
    fireEvent.click(helper.getByTestId(CLOSE_BTN_ID));
    let titleQuery = helper.queryByTestId(TITLE_ID);
    await waitForElementToBeRemoved(() => helper.queryByTestId(TITLE_ID));

    //then:
    expect(titleQuery).not.toBeInTheDocument();
  });
});
