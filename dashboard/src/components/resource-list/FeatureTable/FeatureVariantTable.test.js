// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import '@testing-library/jest-dom/extend-expect';
import { cleanup, render } from '@testing-library/react';
import React from 'react';
import FeatureVariantTable from './FeatureVariantTable';
import { feature_test_response } from './test_data';

const mockData = feature_test_response;

const dataApiMock = {
  getFeatureVariants: jest.fn().mockResolvedValue(mockData),
  getTypeTags: jest.fn().mockResolvedValue([]),
  getTypeOwners: jest.fn().mockResolvedValue([]),
};

jest.mock('../../../hooks/dataAPI', () => ({
  useDataAPI: () => {
    return dataApiMock;
  },
}));

describe('Feature Variant Table Tests', () => {
  beforeEach(() => {});

  afterEach(() => {
    jest.clearAllMocks();
    cleanup();
  });

  test('The feature variant table renders the feature names OK', async () => {
    //given:
    const helper = render(<FeatureVariantTable />);

    //when:
    const firstVariant = await helper.findByText('avg_transactions_OK');
    const secondVariant = await helper.findByText('example');

    // expect:
    expect(firstVariant).toBeInTheDocument();
    expect(secondVariant).toBeInTheDocument();
  });

  test('The table renders the status names OK', async () => {
    //given:
    const helper = render(<FeatureVariantTable />);

    //when:
    const failedStatus = await helper.findByText('FAILED');
    const readyStatus = await helper.findByText('READY');

    // expect:
    expect(failedStatus).toBeInTheDocument();
    expect(readyStatus).toBeInTheDocument();
  });
});
