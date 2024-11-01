// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { Chip } from '@mui/material';
import React from 'react';
import { WHITE } from '../../styles/theme';

export default function StatusChip({ status = '' }) {
  const chipMap = {
    NO_STATUS: '#D0FAEC',
    CREATED: '#D1FAD0',
    PENDING: '#FAECD0',
    READY: '#D1FAD0',
    FAILED: '#FAD0F6',
  };
  return (
    <Chip
      sx={{ backgroundColor: chipMap[status] ?? WHITE }}
      size='medium'
      label={status?.toUpperCase()}
    />
  );
}
