// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import Typography from '@mui/material/Typography';
import { styled } from '@mui/system';
import React from 'react';
import { FEATUREFORM_DARK_GRAY } from '../../styles/theme';

const ListHeader = styled(Typography)(() => ({
  marginTop: 15,
  marginBottom: 0,
  display: 'flex',
  marginLeft: 15,
  color: FEATUREFORM_DARK_GRAY,
}));

export default function SideNavHeader({ displayText = 'empty' }) {
  return <ListHeader variant='body2'>{displayText}</ListHeader>;
}
