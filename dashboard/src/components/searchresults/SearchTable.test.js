// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { cleanup, fireEvent, render, act, waitFor } from '@testing-library/react';
import 'jest-canvas-mock';
import React from 'react';
import Resource from '../../api/resources/Resource';
import SearchTable, { searchTypeMap } from './SearchTable';
import { search_results } from './testData/test_results';

const mockSearchResults = {
  SOURCE: {
    Name: 'average_user_transaction',
    Variant: '2024-04-17t14-59-42',
    Type: 'SOURCE',
    Tags: [],
  },
  SOURCE_VARIANT: {
    Name: 'average_user_transaction_dev',
    Variant: '2024-04-22t18-11-31',
    Type: 'SOURCE_VARIANT',
    Tags: [],
  },
  FEATURE: {
    Name: 'ondemand_add_integers',
    Variant: 'staging test',
    Type: 'FEATURE',
    Tags: [],
  },
  FEATURE_VARIANT: {
    Name: 'ondemand_add_integers_dev',
    Variant: '2024-04-17t13-49-05',
    Type: 'FEATURE_VARIANT',
    Tags: [],
  },
  USER: {
    Name: 'a_user',
    Variant: '',
    Type: 'USER',
    Tags: [],
  },
  ENTITY: {
    Name: 'user',
    Variant: '',
    Type: 'ENTITY',
    Tags: [],
  },
  TRAINING_SET_VARIANT: {
    Name: 'fraud',
    Variant: '2024-04-23t14-33-15',
    Type: 'TRAINING_SET_VARIANT',
    Tags: [],
  },
  LABEL: {
    Name: 'my_label',
    Variant: '2024-04-23t14-33-15',
    Type: 'LABEL',
    Tags: [],
  },
  LABEL_VARIANT: {
    Name: 'my_label_variant',
    Variant: '2024-04-23t14-33-15',
    Type: 'LABEL_VARIANT',
    Tags: [],
  },
  MODEL: {
    Name: 'my_model',
    Variant: '',
    Type: 'MODEL',
    Tags: [],
  },
};

const dataApiMock = {
  searchResources: jest.fn().mockImplementation(() => Promise.resolve([{
    Name: '',
    Variant: '',
    Type: '',
    Tags: []
  }])),
};

jest.mock('../../hooks/dataAPI', () => ({
  useDataAPI: () => dataApiMock,
}));

const userRouterMock = {
  push: jest.fn(),
  query: {},
};

jest.mock('next/router', () => ({
  useRouter: () => userRouterMock,
}));

describe('Search Table Tests', () => {
  const defaultProps = {
    rows: [],
    searchQuery: 'search query',
    setVariant: jest.fn(),
  };

  const getTestBody = (props = defaultProps) => {
    return (
      <>
        <SearchTable {...props} />
      </>
    );
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  test('Basic Table Renders without any rows', async () => {
    //given:
    const noResults = 'no results search';
    const bingbong = render(
      getTestBody({ ...defaultProps, searchQuery: noResults })
    );
    const foundText = bingbong.getByText(`Search Results: ${noResults}`);

    //expect:
    expect(foundText).toBeDefined();
  });

  test('Basic table renders each resource row', async () => {
    //given:
    const props = { ...defaultProps, rows: search_results };

    const bingbong = render(getTestBody(props));

    //then:
    props.rows.forEach((element) => {
      const foundTitle = bingbong.getByText(element.Name);
      expect(foundTitle).toBeDefined();
    });
  });

  test.each`
    ResourceParam             | RouteParam
    ${'SOURCE'}              | ${'/sources/average_user_transaction?variant=2024-04-17t14-59-42'}
    ${'SOURCE_VARIANT'}      | ${'/sources/average_user_transaction_dev?variant=2024-04-22t18-11-31'}
    ${'FEATURE'}             | ${'/features/ondemand_add_integers?variant=staging test'}
    ${'FEATURE_VARIANT'}     | ${'/features/ondemand_add_integers_dev?variant=2024-04-17t13-49-05'}
    ${'USER'}                | ${'/users/a_user'}
    ${'ENTITY'}              | ${'/entities/user'}
    ${'TRAINING_SET_VARIANT'}| ${'/training-sets/fraud?variant=2024-04-23t14-33-15'}
    ${'LABEL'}               | ${'/labels/my_label?variant=2024-04-23t14-33-15'}
    ${'LABEL_VARIANT'}       | ${'/labels/my_label_variant?variant=2024-04-23t14-33-15'}
    ${'MODEL'}               | ${'/models/my_model'}
  `(
    'Clicking a resource row of type ($ResourceParam) routes to the correct address',
    async ({ ResourceParam, RouteParam }) => {
      // given:
      const mockResult = mockSearchResults[ResourceParam];
      dataApiMock.searchResources.mockResolvedValue([mockResult]);

      // when:
      helper = render(<SearchTable />);

      // then:
      expect(userRouterMock.push).toHaveBeenCalledTimes(1);
      expect(userRouterMock.push).toHaveBeenCalledWith(RouteParam);
    }
  );
});
