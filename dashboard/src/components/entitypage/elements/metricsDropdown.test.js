// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { ThemeProvider } from '@mui/material/styles';
import { cleanup, render } from '@testing-library/react';
import 'jest-canvas-mock';
import React from 'react';
import { newTestStore } from '../../../components/redux/store';
import ReduxWrapper from '../../../components/redux/wrapper/ReduxWrapper';
import TEST_THEME from '../../../styles/theme';
import MetricsDropdown from './MetricsDropdown';

jest.mock('./QueryDropdown', () => {
  const comp = () => <div />;
  comp.displayName = 'mock';
  return comp;
});

describe('Metrics Dropdown tests', () => {
  const getTestBody = (type = '') => {
    return (
      <>
        <ReduxWrapper store={newTestStore()}>
          <ThemeProvider theme={TEST_THEME}>
            <MetricsDropdown
              type={type}
              name={'resourceName'}
              variant={'resourceVariant'}
            />
          </ThemeProvider>
        </ReduxWrapper>
      </>
    );
  };

  beforeEach(() => {
    jest.resetAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  test.each`
    TypeParam        | ExpectedSize
    ${'Feature'}     | ${1500}
    ${'TrainingSet'} | ${1000}
  `(
    'The resource type ($TypeParam) sets the correct viewport size ($ExpectedSize)',
    async ({ TypeParam, ExpectedSize }) => {
      //given:
      const helper = render(getTestBody(TypeParam));

      //when:
      const found = await helper.findByTestId('viewPortId');
      const foundStyle = found.getAttribute('style');

      //then:
      expect(found.nodeName).toBe('DIV');
      expect(foundStyle).toContain(`height: ${ExpectedSize}px`);
    }
  );
});
