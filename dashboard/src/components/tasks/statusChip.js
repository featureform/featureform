// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { Chip } from '@mui/material';
import React from 'react';
import { WHITE } from '../../styles/theme';

export default function StatusChip({ status }) {
  const chipMap = {
    3: '#D1FAD0',
    2: '#FAECD0',
    1: '#FAECD0',
    4: '#FAD0F6',
    5: '#D6D0FA',
  };
  const STATUS_MAP = {
    0: 'No Status',
    1: 'Created',
    2: 'Pending',
    3: 'Ready',
    4: 'Failed',
    5: 'Running',
  };
  return (
    <Chip
      sx={{ backgroundColor: chipMap[status] ?? WHITE }}
      size='small'
      label={STATUS_MAP[status] ?? 'No Status'}
    />
  );
}
