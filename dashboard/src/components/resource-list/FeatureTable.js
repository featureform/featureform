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
import BaseTable, { getDefaultVariant } from './BaseTable';
import { ConnectionSvg } from './icons/Connections';
import { UserBubbleSvg } from './icons/Owner';

export const feature_columns = [
  {
    field: 'id',
    headerName: 'id',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: true,
  },
  {
    field: 'name',
    headerName: 'Name',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function (params) {
      const variant = getDefaultVariant(params.row);
      return (
        <>
          <Typography variant='body1' sx={{ marginLeft: 1 }}>
            {variant?.name}
          </Typography>
        </>
      );
    },
  },
  {
    field: 'provider',
    headerName: 'Provider',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function (params) {
      const variant = getDefaultVariant(params.row);
      return (
        <>
          <Typography variant='body1' sx={{ marginLeft: 1 }}>
            {variant?.provider}
          </Typography>
        </>
      );
    },
  },
  {
    field: 'tags',
    headerName: 'Tags',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function (params) {
      const variant = getDefaultVariant(params.row);
      return (
        <div>
          <div style={{ display: 'flex' }}></div>
          <div style={{ display: 'flex' }}>
            <Box>
              {variant?.tags?.slice(0, 3).map((tag) => (
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
          </div>
        </div>
      );
    },
  },
  {
    field: 'status',
    headerName: 'Status',
    flex: 0,
    width: 350,
    editable: false,
    sortable: false,
    filterable: false,
    renderCell: function (params) {
      const variant = getDefaultVariant(params.row);
      const readyFill = '#6DDE6A';
      let result = '#DA1E28';
      if (variant?.status && variant?.status === 'READY') {
        result = readyFill;
      }
      return (
        <div>
          <div style={{ display: 'flex' }}>
            <ConnectionSvg fill={result} height='20' width='20' />
            <Typography variant='body1' sx={{ marginLeft: 1 }}>
              {variant?.status}
            </Typography>
          </div>
          <div style={{ display: 'flex' }}>
            <UserBubbleSvg
              height='20'
              width='20'
              letter={variant?.owner?.[0]?.toUpperCase()}
            />
            <Typography variant='body1' sx={{ marginLeft: 1 }}>
              {variant?.owner}
            </Typography>
          </div>
        </div>
      );
    },
  },
];

export const FeatureTable = ({
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
    router.push(`/features/${name}`);
  };

  return (
    <BaseTable
      title={'Features'}
      type={type}
      columns={feature_columns}
      resources={rows}
      loading={loading}
      redirect={redirect}
      count={count}
      currentPage={currentPage}
      setPage={setPage}
    />
  );
};

export default FeatureTable;
