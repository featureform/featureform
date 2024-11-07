// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { Box } from '@mui/material';
import { styled } from '@mui/system';
import { DataGrid } from '@mui/x-data-grid';

const tableHeight = 650;

export const MainContainer = styled(Box)({
  display: 'flex',
});

export const GridContainer = styled(Box)({
  width: '100%',
  marginLeft: 20,
});

export const StyledDataGrid = styled(DataGrid)(() => ({
  height: tableHeight,
  '& .MuiDataGrid-cell:focus': {
    outline: 'none',
  },
  '& .MuiDataGrid-columnHeader:focus': {
    outline: 'none',
  },
  '& .MuiDataGrid-colCellWrapper': {
    display: 'none',
  },
  marginBottom: '1.5em',
  cursor: 'pointer',
  '& .MuiDataGrid-columnHeaderTitle': {
    fontWeight: 'bold',
  },
}));
