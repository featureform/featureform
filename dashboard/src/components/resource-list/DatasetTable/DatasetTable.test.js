// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import '@testing-library/jest-dom/extend-expect';
import { cleanup, render } from '@testing-library/react';
import React from 'react';
import DatasetTable from './DatasetTable';
import { test_data } from './test_data';

const mockData = { ...test_data };

const dataApiMock = {
  getSourceVariants: jest.fn().mockResolvedValue(mockData),
  getTypeTags: jest.fn().mockResolvedValue([]),
  getTypeOwners: jest.fn().mockResolvedValue([]),
};

jest.mock('../../../hooks/dataAPI', () => ({
  useDataAPI: () => {
    return dataApiMock;
  },
}));

describe('Dataset Table Tests', () => {
  beforeEach(() => {});

  afterEach(() => {
    jest.clearAllMocks();
    cleanup();
  });

  test('The dataset variant table renders the variant names OK', async () => {
    //given:
    const helper = render(<DatasetTable />);

    //when:
    const firstVariant = await helper.findByText('average_user_transaction');
    const secondVariant = await helper.findByText('transactions');

    // expect:
    expect(firstVariant).toBeInTheDocument();
    expect(secondVariant).toBeInTheDocument();
  });

  test('The table renders the status names OK', async () => {
    //given:
    const helper = render(<DatasetTable />);

    //when:
    const readyStatuses = await helper.findAllByText('READY');

    // expect:
    expect(readyStatuses.length).toBe(mockData.count);
  });

  test('The table renders the tag names correctly', async () => {
    //given:
    const helper = render(<DatasetTable />);

    //when:
    const recordTags1 = await helper.findByText('avg, users');
    const recordTags2 = await helper.findByText('my_data, m2, haha');

    // expect:
    expect(recordTags1).toBeInTheDocument();
    expect(recordTags2).toBeInTheDocument();
  });
});
