import { ThemeProvider } from '@mui/material/styles';
import { act, cleanup, fireEvent, render } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import 'jest-canvas-mock';
import React from 'react';
import TEST_THEME from '../../../styles/theme';
import TagBox from './TagBox';

const dataAPIMock = { postTags: jest.fn(), getTags: jest.fn() };

jest.mock('../../../hooks/dataAPI', () => ({
  useDataAPI: () => {
    return dataAPIMock;
  },
}));

describe('Tag Box Tests', () => {
  const TAG_INPUT_ID = 'tagInputId';
  const CHIP_DELETE_ID = 'CancelIcon';
  const TAG_DISPLAY_FIELD_BTN = 'displayTextBtnId';
  const USER_EVENT_ENTER = '{enter}';
  const TEST_TITLE = 'test title';
  const TEST_TYPE = 'test type';
  const TEST_VARIANT = 'myVariant';
  const TEST_RESOURCE_NAME = 'test resource name';

  const INPUT_NODE = 'INPUT';
  const SPAN_NODE = 'SPAN';

  const getTestBody = (tags = []) => {
    return (
      <>
        <ThemeProvider theme={TEST_THEME}>
          <TagBox
            type={TEST_TYPE}
            variant={TEST_VARIANT}
            resourceName={TEST_RESOURCE_NAME}
            title={TEST_TITLE}
            tags={tags}
          />
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

  test('Clicking the open button reveals the tag input field', async () => {
    //given:
    const helper = render(getTestBody());

    //when:
    const displayBtn = helper.getByTestId(TAG_DISPLAY_FIELD_BTN);
    fireEvent.click(displayBtn);
    const foundInputField = helper.getByTestId(TAG_INPUT_ID);

    //then:
    expect(foundInputField.nodeName).toBe(INPUT_NODE);
    expect(foundInputField.value).toBe('');
  });

  test('Clicking the close button hides the tag input field', async () => {
    //given:
    const helper = render(getTestBody());
    const displayBtn = helper.getByTestId(TAG_DISPLAY_FIELD_BTN);
    fireEvent.click(displayBtn);
    helper.getByTestId(TAG_INPUT_ID);

    //when:
    fireEvent.click(displayBtn);
    const foundInputField = helper.queryByTestId(TAG_INPUT_ID);

    //then:
    expect(foundInputField).toBeNull();
  });

  test('Hitting enter with a new tag name invokes the update tags api', async () => {
    //given:
    const newTag = 'newTag';
    const helper = render(getTestBody());
    fireEvent.click(helper.getByTestId(TAG_DISPLAY_FIELD_BTN));

    //when: the user types a new tag name, and hits enter
    const foundInputField = helper.getByTestId(TAG_INPUT_ID);
    await userEvent.type(foundInputField, `${newTag}${USER_EVENT_ENTER}`);

    //then: the api is invoked
    expect(dataAPIMock.postTags).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.postTags).toHaveBeenCalledWith(
      TEST_TYPE,
      TEST_RESOURCE_NAME,
      TEST_VARIANT,
      [newTag]
    );
  });

  test('Pre-existing tag list renders OK', async () => {
    //given: an existing list
    const existingTags = ['customTag1', 'customTag2', 'customTag3'];
    const helper = render(getTestBody(existingTags));

    //expect: each tag to be present
    existingTags.forEach((tagName) => {
      const foundTag = helper.getByText(tagName, { exact: false });
      expect(foundTag.nodeName).toBe(SPAN_NODE);
    });
  });

  test('Deleting a tag invokes the update tags api without the tag', async () => {
    //given: an existing tag list with 1 item
    const existingTags = ['deleteMe1'];
    const helper = render(getTestBody(existingTags));

    //when:
    const foundDelete = await helper.findByTestId(CHIP_DELETE_ID);
    fireEvent.click(foundDelete);

    //then: the update api is invoked and does not include the deleted tag
    expect(dataAPIMock.postTags).toHaveBeenCalledTimes(1);
    expect(dataAPIMock.postTags).toHaveBeenCalledWith(
      TEST_TYPE,
      TEST_RESOURCE_NAME,
      TEST_VARIANT,
      [] //empty list
    );
  });

  test('After a new tag is added, the pre-existing list is updated', async () => {
    //given: an existing list
    const existingTags = ['original tag 1', 'original tag 2'];
    const newTag = 'brandNewTag';
    const updatedTags = [...existingTags, newTag];
    dataAPIMock.postTags.mockResolvedValueOnce({
      tags: updatedTags,
    });
    const helper = render(getTestBody(existingTags));
    fireEvent.click(helper.getByTestId(TAG_DISPLAY_FIELD_BTN));

    //when: the user types a new tag name, and hits enter
    const foundInputField = helper.getByTestId(TAG_INPUT_ID);
    await act(() =>
      userEvent.type(foundInputField, `${newTag}${USER_EVENT_ENTER}`)
    );

    //expect: the old tags and new tag are present
    updatedTags.forEach((tagName) => {
      const foundTag = helper.getByText(tagName, { exact: false });
      expect(foundTag.nodeName).toBe(SPAN_NODE);
    });
  });
});
