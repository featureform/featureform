// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import CssBaseline from '@mui/material/CssBaseline';
import { ThemeProvider } from '@mui/material/styles';
import React from 'react';
import MainContainer from '../src/components/app/MainContainer';
import ReduxStore from '../src/components/redux/store';
import ReduxWrapper from '../src/components/redux/wrapper';
import '../src/styles/base.css';
import theme from '../src/styles/theme';

export const MyApp = ({ Component, pageProps: { ...pageProps } }) => {
  return (
    <React.StrictMode>
      <ReduxWrapper store={ReduxStore}>
        <ThemeWrapper>
          <MainContainer Component={Component} pageProps={pageProps} />
        </ThemeWrapper>
      </ReduxWrapper>
    </React.StrictMode>
  );
};

export const views = {
  RESOURCE_LIST: 'ResourceList',
  EMPTY: 'Empty',
};

export const ThemeWrapper = ({ children }) => (
  <ThemeProvider theme={theme}>
    <CssBaseline />
    {children}
  </ThemeProvider>
);

export default MyApp;

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA
// serviceWorker.unregister();
