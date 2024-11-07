// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { Box } from '@mui/material';
import { styled } from '@mui/system';
import { DataGrid } from '@mui/x-data-grid';
import React from 'react';
import NoDataMessage from './NoDataMessage';
import { DEFAULT_RESOURCE_LIST_SIZE } from './ResourceList';

export function getDefaultVariant(rowRecord) {
  let variant = {};
  if (rowRecord) {
    const defaultVariant = rowRecord['default-variant'];
    variant = rowRecord.variants[defaultVariant];
  }
  return variant;
}

const StyledDataGrid = styled(DataGrid)(() => ({
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

const noDataHeight = 300;

const MainContainer = styled(Box)({
  display: 'flex',
});

const GridContainer = styled(Box)({
  width: '100%',
});

export const BaseTable = ({
  title,
  columns,
  resources,
  type,
  loading = true,
  count,
  currentPage,
  setPage = () => null,
  redirect = () => null,
}) => {
  const rows = resources ?? [];

  const isEmpty = () => {
    return !loading && rows?.length === 0;
  };

  return (
    <>
      <h3>{title}</h3>
      <MainContainer>
        <GridContainer>
          {loading ? (
            <div data-testid='loadingGrid'>
              <StyledDataGrid
                disableVirtualization
                aria-label={title}
                autoHeight
                density='compact'
                loading={loading}
                rows={[]}
                rowCount={0}
                columns={columns}
                hideFooterSelectedRowCount
                disableColumnFilter
                disableColumnMenu
                disableColumnSelector
                paginationMode='server'
                rowsPerPageOptions={[DEFAULT_RESOURCE_LIST_SIZE]}
              />
            </div>
          ) : (
            <StyledDataGrid
              disableVirtualization
              autoHeight={!isEmpty()}
              sx={
                isEmpty()
                  ? {
                      minHeight: noDataHeight,
                    }
                  : {}
              }
              aria-label={title}
              rows={rows}
              columns={columns}
              density='compact'
              rowHeight={80}
              hideFooterSelectedRowCount
              disableColumnFilter
              disableColumnMenu
              disableColumnSelector
              paginationMode='server'
              sortModel={[{ field: 'name', sort: 'asc' }]}
              onPageChange={(newPage) => {
                setPage(type, newPage);
              }}
              components={{
                NoRowsOverlay: () => <NoDataMessage type={type} />,
              }}
              rowCount={count}
              page={currentPage}
              pageSize={DEFAULT_RESOURCE_LIST_SIZE}
              rowsPerPageOptions={[DEFAULT_RESOURCE_LIST_SIZE]}
              onRowClick={(params, event) => {
                event?.preventDefault();
                if (params?.row?.name) {
                  redirect(params.row.name);
                }
              }}
              getRowId={(row) => row.name}
            />
          )}
        </GridContainer>
      </MainContainer>
    </>
  );
};

export default BaseTable;
