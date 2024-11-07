// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { cleanup, render } from '@testing-library/react';
import 'jest-canvas-mock';
import React from 'react';
import SideNavIcon from './SideNavIcon';

describe('Side Nav Icon tests', () => {
  const getTestBody = (key = '') => {
    return <SideNavIcon iconKey={key} isSelected={true} />;
  };

  beforeEach(() => {
    jest.resetAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  test.each`
    IconKeyParam        | ExpectedFillParam
    ${'home'}           | ${'#ED215E'}
    ${'storage'}        | ${'#DA1E28'}
    ${'description'}    | ${'#EF4444'}
    ${'fingerprint'}    | ${'#F59E0B'}
    ${'label'}          | ${'#F97316'}
    ${'model_training'} | ${'#EAB308'}
    ${'device_hub'}     | ${'#8B5CF6'}
    ${'tasks'}          | ${'#4ADE80'}
    ${'person'}         | ${'#22D3EE'}
    ${'engineer'}       | ${'#0EA5E9'}
    ${'group'}          | ${'#3B82F6'}
    ${'exit'}           | ${'#6c6e72'}
    ${'default'}        | ${'#6c6e72'}
  `(
    `The "$IconKeyParam" svg renders the correct selected fill hex: $ExpectedFillParam`,
    async ({ IconKeyParam, ExpectedFillParam }) => {
      //given:
      const helper = render(getTestBody(IconKeyParam));
      const SVG_ID = 'svgId-' + IconKeyParam;

      //when:
      const foundIcon = helper.getByTestId(SVG_ID);
      const foundFill = foundIcon.getAttribute('fill');

      //then:
      expect(foundFill).toBe(ExpectedFillParam);
    }
  );
});
