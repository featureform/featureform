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
import Resource from '../../api/resources/Resource';
import BaseTable from './BaseTable';
import TypeIcon from './TypeIcon';

export const searchTypeMap = Object.freeze({
  FEATURE: 'Feature',
  FEATURE_VARIANT: 'Feature',
  LABEL: 'Label',
  LABEL_VARIANT: 'Label',
  TRAINING_SET: 'TrainingSet',
  TRAINING_SET_VARIANT: 'TrainingSet',
  SOURCE: 'Source',
  SOURCE_VARIANT: 'Source',
  PROVIDER: 'Provider',
  ENTITY: 'Entity',
  MODEL: 'Model',
  USER: 'User',
});

export const searchColumns = [
  {
    field: 'Name',
    headerName: 'Name',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function (params) {
      return (
        <>
          <Typography variant='body1' sx={{ marginLeft: 1 }}>
            {params.row?.Name ?? ''}
          </Typography>
        </>
      );
    },
  },
  {
    field: 'Type',
    headerName: 'Type',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function (params) {
      const resourceType =
        Resource[searchTypeMap[params.row.Type?.toUpperCase()]];
      return (
        <>
          <TypeIcon iconKey={params.row?.Type} />
          <Typography variant='body1' sx={{ marginLeft: 1 }}>
            {resourceType.typePlural ?? ''}
          </Typography>
        </>
      );
    },
  },
  {
    field: 'Variant',
    headerName: 'Variant',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function (params) {
      return (
        <>
          <Typography variant='body1' sx={{ marginLeft: 1 }}>
            {params.row?.Variant ?? ''}
          </Typography>
        </>
      );
    },
  },
  {
    field: 'Tags',
    headerName: 'Tags',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function (params) {
      return (
        <div>
          <div style={{ display: 'flex' }}>
            <Box>
              {params.row?.Tags?.slice(0, 3).map((tag) => (
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
];

export const SearchTable = ({ rows, searchQuery = '', setVariant }) => {
  const resultRows = rows ?? [];
  const router = useRouter();

  function handleClick(name = '', variant = '', type = '') {
    const resourceType = Resource[searchTypeMap[type?.toUpperCase()]];
    if (resourceType?.hasVariants) {
      setVariant?.(type, name, variant);
      const base = resourceType.urlPathResource(name);
      router.push(`${base}?variant=${variant}`);
    } else {
      router.push(resourceType.urlPathResource(name));
    }
  }

  return (
    <BaseTable
      title={'Search Results: ' + searchQuery}
      columns={searchColumns}
      rows={resultRows}
      redirect={handleClick}
    />
  );
};

export default SearchTable;
