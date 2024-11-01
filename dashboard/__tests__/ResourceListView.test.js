// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { cleanup, render } from '@testing-library/react';
import produce from 'immer';
import 'jest-canvas-mock';
import React from 'react';
import {
  filterMissingDefaults,
  ResourceListView,
} from '../src/components/resource-list/ResourceListView';
import { deepCopy } from '../src/helper';

describe('ResourceListView tests', () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  const PROGRESS_BAR = 'progressbar';
  const SVG_NODE = 'svg';
  const SPAN_NODE = 'SPAN';
  const DIV_NODE = 'DIV';
  const B_NODE = 'B';

  test('The resource list renders correctly when no data is present', () => {
    //given:
    const helper = render(<ResourceListView title='test' type='Feature' />);

    //when:
    const foundTitle = helper.getByText('Features');

    //then:
    expect(foundTitle.nodeName).toBe(B_NODE);
  });

  test('The resource list correctly renders the name and description columns', () => {
    //given: a row with data
    const helper = render(
      <ResourceListView
        title='test'
        type='Feature'
        resources={[
          {
            name: 'abc',
            description: 'my description',
            revision: 'Invalid Date',
            'default-variant': 'first-variant',
          },
        ]}
      />
    );

    //when:
    const foundName = helper.getByText('abc');
    const foundDesc = helper.getByText('my description');

    //then:
    expect(foundName.nodeName).toBe(DIV_NODE);
    expect(foundDesc.nodeName).toBe(DIV_NODE);
  });

  test('deepCopy takes an immutable object and makes it mutable', () => {
    //given:
    const immutData = produce([], (draft) => {
      draft.push({
        name: 'abc',
        'default-variant': 'first-variant',
        variants: { 'first-variant': {}, 'second-variant': {} },
      });
    });

    //when:
    const mutableCopy = deepCopy(immutData);
    mutableCopy[0].name = 'change from the original name';

    //then:
    expect(Object.isFrozen(immutData)).toBeTruthy();
    expect(immutData[0].name).not.toBe(mutableCopy[0].name);
    expect(Object.isExtensible(mutableCopy)).toBeTruthy();
  });

  test('When resources are empty, set isLoading to true and verify the progress bar', async () => {
    //given:
    const helper = render(<ResourceListView title='test' type='Feature' />);

    //when:
    const foundProgressBar = await helper.findByRole(PROGRESS_BAR);

    //then:
    expect(foundProgressBar.nodeName).toBe(SPAN_NODE);
    expect(foundProgressBar.firstChild.nodeName).toBe(SVG_NODE);
  });

  test('When the loading prop is true, ensure the progress bar is rendered', async () => {
    const helper = render(
      <ResourceListView title='test' type='Feature' loading={true} />
    );
    const foundProgressBar = await helper.findByRole(PROGRESS_BAR);

    expect(foundProgressBar.nodeName).toBe(SPAN_NODE);
    expect(foundProgressBar.firstChild.nodeName).toBe(SVG_NODE);
  });

  test('When failed, set isLoading to true', async () => {
    const helper = render(
      <ResourceListView
        title='test'
        loading={false}
        failed={true}
        type='Feature'
      />
    );
    const foundProgressBar = await helper.findByRole(PROGRESS_BAR);

    expect(foundProgressBar.nodeName).toBe(SPAN_NODE);
    expect(foundProgressBar.firstChild.nodeName).toBe(SVG_NODE);
  });

  test('Issue-204: Filter out resources that are missing their default variant in their all-variants list.', async () => {
    console.warn = jest.fn();
    const missingDefault = 'MISSING DEFAULT!!!';
    const variantList = ['eloquent_goldstine', 'sleepy_volhard'];
    //the default variant is missing from the global list
    const badJsonResponse = [
      {
        'all-variants': variantList,
        type: 'Source',
        'default-variant': missingDefault,
        name: 'transactions',
        variants: {
          eloquent_goldstine: {
            name: 'transactions',
            variant: 'eloquent_goldstine',
          },
          sleepy_volhard: {
            name: 'transactions',
            variant: 'sleepy_volhard',
          },
        },
      },
    ];

    const helper = render(
      <ResourceListView
        title='Source'
        loading={false}
        failed={false}
        type='Source'
        resources={badJsonResponse}
      />
    );

    const foundTitle = await helper.findByText('Datasets');
    const foundNoDataContainer = await helper.findByTestId('noDataContainerId');

    expect(foundTitle.nodeName).toBeDefined();
    expect(foundNoDataContainer.nodeName).toBeDefined();
    expect(console.warn).toHaveBeenCalledWith(
      `The current default rowVariant (${missingDefault}) is not present in the variants list:`,
      variantList
    );
  });

  test('Issue-204: removeResourcesWithMissingDefaults() removes resources whose default names are not present in the "all-variants"', async () => {
    console.warn = jest.fn();
    //the second record, is missing it's default variant from the all-variants list
    const jsonResponse = [
      {
        'all-variants': ['presentVariant', 'secondVariant'],
        type: 'Source',
        'default-variant': ['presentVariant'],
        name: 'good record',
        variants: {
          presentVariant: {
            name: 'transactions',
            variant: 'presentVariant',
          },
          secondVariant: {
            name: 'transactions',
            variant: 'secondVariant',
          },
        },
      },
      {
        'all-variants': ['eloquent_goldstine', 'sleepy_volhard'],
        type: 'Source',
        'default-variant': 'MISSING VARIANT!',
        name: 'faulty record',
        variants: {
          eloquent_goldstine: {
            name: 'transactions',
            variant: 'eloquent_goldstine',
          },
          sleepy_volhard: {
            name: 'transactions',
            variant: 'sleepy_volhard',
          },
        },
      },
    ];

    const result = jsonResponse.filter(filterMissingDefaults);

    expect(result.length).toBe(1);
    expect(result[0].name).toBe('good record');
  });
});
