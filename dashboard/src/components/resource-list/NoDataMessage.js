// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { Button, Container, Typography } from '@mui/material';
import { styled } from '@mui/system';
import React from 'react';
import Resource from '../../api/resources/Resource';

const NoDataPage = styled('div')(({ theme }) => ({
  '& > *': {
    padding: theme.spacing(1),
  },
  textAlign: 'center',
}));

export default function NoDataMessage({
  type = 'resourceType',
  usingFilters = false,
}) {
  function redirect() {
    const url = 'https://docs.featureform.com/getting-started/overview';
    const newWindow = window.open(url, '_blank', 'noopener,noreferrer');
    if (newWindow) newWindow.opener = null;
  }

  return (
    <>
      <Container data-testid='noDataContainerId'>
        {usingFilters ? (
          <>
            <NoDataPage>
              <Typography variant='h4'>
                No {Resource[type]?.typePlural ?? 'resource'} found with the
                given filters!
              </Typography>
              <Typography variant='body1'>
                Check out our docs for step by step instructions to create more.
              </Typography>
              <Button variant='outlined' onClick={redirect}>
                FeatureForm Docs
              </Button>
            </NoDataPage>
          </>
        ) : (
          <>
            <NoDataPage>
              <Typography variant='h4'>
                No {Resource[type]?.typePlural ?? 'resource'} Registered
              </Typography>
              <Typography variant='body1'>
                There are no visible {type?.toLowerCase() ?? 'resource'}s in
                your organization.
              </Typography>
              <Typography variant='body1'>
                Check out our docs for step by step instructions to create one.
              </Typography>
              <Button variant='outlined' onClick={redirect}>
                FeatureForm Docs
              </Button>
            </NoDataPage>
          </>
        )}
      </Container>
    </>
  );
}
