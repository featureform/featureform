// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import ReactEighteenAdapter from '@cfaester/enzyme-adapter-react-18';
import { configure, mount } from 'enzyme';
import 'jest-canvas-mock';
import React from 'react';
import { testData } from '../src/api/resources';
import { newTestStore } from '../src/components/redux/store';
import ReduxWrapper from '../src/components/redux/wrapper';
import ResourceList, {
  selectFilteredResources,
} from '../src/components/resource-list/ResourceList';

configure({ adapter: new ReactEighteenAdapter() });

jest.mock('next/dynamic', () => () => {
  const ResourceListView =
    // eslint-disable-next-line global-require
    require('../src/components/resource-list/ResourceListView')
      .ResourceListView;

  return (props) => <ResourceListView {...props} />;
});

describe('ResourceList', () => {
  const wrapInPromise = (arr) => Promise.resolve({ data: arr });
  const dataType = 'Feature';
  const mockFn = jest.fn(() => wrapInPromise(testData));
  const apiMock = {
    fetchResources: mockFn,
  };

  const component = mount(
    <ReduxWrapper store={newTestStore()}>
      <ResourceList api={apiMock} type={dataType} />
    </ReduxWrapper>
  );

  it('fetches resources on mount.', () => {
    expect(mockFn.mock.calls.length).toBe(1);
    expect(mockFn.mock.calls[0][0]).toEqual(dataType);
  });

  it.skip('correctly maps inital props from state.', async () => {
    const listComponent = component.find('ResourceListView');
    expect(listComponent.length).toBe(1);
    const viewProps = listComponent.props();
    expect(viewProps).toMatchObject({
      activeVariants: {},
      title: dataType,
      resources: null,
      loading: true,
      failed: false,
    });
    const expKeys = [
      'activeTags',
      'activeVariants',
      'title',
      'resources',
      'loading',
      'setCurrentType',
      'type',
      'failed',
      'setVariant',
      'toggleTag',
    ];
    expect(Object.keys(viewProps).sort()).toEqual(expKeys.sort());
  });

  describe('Resource Filter', () => {
    it("returns null when resources isn't set", () => {
      const state = {
        resourceList: { [dataType]: [] },
        selectedTags: { [dataType]: {} },
        selectedVariant: { [dataType]: {} },
      };
      const selector = selectFilteredResources(dataType);
      expect(selector(state)).toBeNull();
    });

    it("doesn't filter when no tags are selected", () => {
      const resList = [{ name: 'a', tags: ['1', '2'] }, { name: 'b' }];
      const state = {
        resourceList: { [dataType]: { resources: resList } },
        selectedTags: { [dataType]: {} },
        selectedVariant: { [dataType]: {} },
      };
      const selector = selectFilteredResources(dataType);
      expect(selector(state)).toEqual(resList);
    });

    it('filters using tag', () => {
      const resList = [
        { name: 'a', variants: { a1: { tags: ['1', '2'] } } },
        { name: 'b', variants: { b1: { tags: [] } } },
        { name: 'c', variants: { c1: { tags: ['1'] } } },
        { name: 'd', variants: { d1: { tags: ['2'] } } },
      ];
      const state = {
        resourceList: { [dataType]: { resources: resList } },
        selectedTags: { [dataType]: { 1: true } },
        selectedVariant: { [dataType]: { a: 'a1', b: 'b1', c: 'c1', d: 'd1' } },
      };
      const selector = selectFilteredResources(dataType);
      const expected = [0, 2].map((idx) => resList[idx]);
      expect(selector(state)).toEqual(expected);
    });

    it('filters using multiple tags', () => {
      const resList = [
        { name: 'a', variants: { a1: { tags: ['1', '2'] } } },
        { name: 'b', variants: { b1: { tags: [] } } },
        { name: 'c', variants: { c1: { tags: ['1'] } } },
        { name: 'd', variants: { d1: { tags: ['2'] } } },
      ];
      const state = {
        resourceList: { [dataType]: { resources: resList } },
        selectedTags: { [dataType]: { 1: true, 2: true } },
        selectedVariant: { [dataType]: { a: 'a1', b: 'b1', c: 'c1', d: 'd1' } },
      };
      const selector = selectFilteredResources(dataType);
      const expected = [0].map((idx) => resList[idx]);
      expect(selector(state)).toEqual(expected);
    });
  });
});
