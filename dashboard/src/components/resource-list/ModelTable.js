// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { Box, Chip } from '@mui/material';
import Typography from '@mui/material/Typography';
import { useRouter } from 'next/router';
import React from 'react';
import BaseTable from './BaseTable';
import { ConnectionSvg } from './icons/Connections';

export const model_columns = [
  {
    field: 'id',
    headerName: 'id',
    flex: 1,
    width: 150,
    editable: false,
    sortable: false,
    filterable: false,
    hide: true,
  },
  {
    field: 'name',
    headerName: 'Name',
    flex: 1,
    width: 150,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function (params) {
      return (
        <>
          <Typography variant='body1' sx={{ marginLeft: 1 }}>
            {params.row?.name}
          </Typography>
        </>
      );
    },
  },
  {
    field: 'tags',
    headerName: 'Tags',
    flex: 1,
    width: 150,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function (params) {
      return (
        <>
          <Box>
            {params.row?.tags?.slice(0, 3).map((tag) => (
              <Chip
                label={tag}
                key={tag}
                data-testid={tag + 'id'}
                sx={{
                  margin: '0.1em',
                  border: '1px solid #F2BB51',
                  color: '#F2BB51',
                  cursor: 'pointer',
                }}
                variant='outlined'
              />
            ))}
          </Box>
        </>
      );
    },
  },
  {
    field: 'status',
    headerName: 'Status',
    flex: 1,
    width: 150,
    editable: false,
    sortable: false,
    filterable: false,
    renderCell: function (params) {
      const readyFill = '#6DDE6A';
      let result = '#DA1E28';
      if (
        params?.row?.status &&
        ['READY', 'CREATED'].includes(params?.row?.status)
      ) {
        result = readyFill;
      }
      return (
        <div style={{ display: 'flex' }}>
          <ConnectionSvg fill={result} height='20' width='20' />
          <Typography variant='body1' sx={{ marginLeft: 1 }}>
            {params?.row?.status}
          </Typography>
        </div>
      );
    },
  },
];

export const ModelTable = ({
  resources,
  type,
  loading,
  count,
  currentPage,
  setPage,
}) => {
  let router = useRouter();
  const rows = resources ?? [];

  const redirect = (name = '') => {
    router.push(`/models/${name}`);
  };

  return (
    <>
      <BaseTable
        title={'Models'}
        type={type}
        columns={model_columns}
        resources={rows}
        loading={loading}
        redirect={redirect}
        count={count}
        currentPage={currentPage}
        setPage={setPage}
      />
    </>
  );
};

export default ModelTable;
