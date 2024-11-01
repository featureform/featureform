// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import Container from '@mui/material/Container';
import { styled } from '@mui/system';
import React from 'react';
import ResourcesAPI from '../../api/resources/Resources';
import SideNav from '../sideNav/SideNav';
import TopBar from '../topbar/TopBar';

const apiHandle = new ResourcesAPI();

const MainContainerStyled = styled(Container)(() => ({
  width: '80%',
  top: '72px',
  left: 210,
  position: 'absolute',
}));

export default function MainContainer({ Component, pageProps }) {
  return (
    <>
      <TopBar api={apiHandle} />
      <SideNav />
      <MainContainerStyled maxWidth='xl' disableGutters>
        <Component {...pageProps} api={apiHandle} />
      </MainContainerStyled>
    </>
  );
}
