// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import React from 'react';
import TableDataWrapper from './tableDataWrapper';
import { styled } from '@mui/system';

const PageContainer = styled('div')(() => ({
  padding: 20,
}));

export default function TaskRunsPage({
  name = '',
  variant = '',
}) {
  return (
    <PageContainer>
      <h3>Task Runs</h3>
      <TableDataWrapper taskName={name} taskVariant={variant}/>
    </PageContainer>
  );
}
