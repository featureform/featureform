// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import Container from '@mui/material/Container';
import CssBaseline from '@mui/material/CssBaseline';
import { styled, ThemeProvider } from '@mui/system';
import React from 'react';
import theme from '../../styles/theme';
import BreadCrumbs from '../breadcrumbs/BreadCrumbs';
import HeaderBar from '../headerBar/HeaderBar';

const PageContainer = styled(Container)(({ theme }) => ({
  paddingLeft: theme.spacing(8),
  paddingRight: theme.spacing(8),
}));

export const App = ({ Component, pageProps }) => {
  return (
    <ThemeWrapper>
      <HeaderBar />
      <PageContainer maxWidth='xl'>
        <BreadCrumbs />
        <Component {...pageProps} />
      </PageContainer>
    </ThemeWrapper>
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

export default App;
