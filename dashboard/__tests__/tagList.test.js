// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { cleanup, fireEvent, render } from '@testing-library/react';
import 'jest-canvas-mock';
import React from 'react';
import { TagList } from '../src/components/resource-list/ResourceListView';

describe('Tag list test', () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  const exampleTags = ['tag1', 'tag2', 'tag3'];
  const CONTAINER_ID = 'tagContainerId';

  const MUI_DEFAULT_CLASS = 'MuiChip-colorDefault';
  const MUI_ACTIVE_CLASS = 'MuiChip-colorSecondary';

  test('Renders correctly with no tags', () => {
    //given:
    const helper = render(<TagList />);

    //when:
    const foundContainer = helper.getByTestId(CONTAINER_ID);

    //then:
    expect(foundContainer.childElementCount).toBe(0);
  });

  test('Highlights active tags', () => {
    //given: the 2nd tag is highlighted
    const helper = render(
      <TagList activeTags={{ [exampleTags[1]]: true }} tags={exampleTags} />
    );

    //when:
    const activeTag = helper.getByText(exampleTags[1]);
    const inactiveTag = helper.getByText(exampleTags[0]);

    //then:
    expect(activeTag.parentNode.className).toContain(MUI_ACTIVE_CLASS);
    expect(inactiveTag.parentNode.className).toContain(MUI_DEFAULT_CLASS);
  });

  test('The toggle function prop is called when clicked', () => {
    //given:
    const toggleMock = jest.fn();
    const firstTag = exampleTags[0];
    const helper = render(
      <TagList tags={exampleTags} toggleTag={toggleMock} />
    );

    //when:
    const foundTag = helper.getByTestId(`${firstTag}-0`);
    fireEvent.click(foundTag);

    //then:
    expect(toggleMock).toHaveBeenCalledTimes(1);
    expect(toggleMock).toHaveBeenCalledWith(firstTag);
  });
});
