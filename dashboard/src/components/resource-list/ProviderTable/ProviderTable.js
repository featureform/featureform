// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { Box, Chip, Typography } from '@mui/material';
import { useRouter } from 'next/router';
import React, { useCallback, useEffect, useState } from 'react';
import { providerLogos } from '../../../api/resources';
import { useDataAPI } from '../../../hooks/dataAPI';
import { MainContainer, GridContainer, StyledDataGrid, STATUS_COLORS } from '../BaseColumnTable';
import BaseFilterPanel from '../BaseFilterPanel';
import { isMatchingDefault } from '../DatasetTable/DatasetTable';
import { ConnectionSvg } from '../icons/Connections';
import { UserBubbleSvg } from '../icons/Owner';
import NoDataMessage from '../NoDataMessage';
import FilterPanel from './FilterPanel';

const FILE_STORE_TYPES = Object.freeze([
  'MEMORY',
  'LOCAL_FILESYSTEM',
  'AZURE',
  'GLUE',
  'S3',
  'GCS',
  'HDFS',
]);

const PROVIDER_STATUS = Object.freeze({
  READY: 'READY',
  CREATED: 'CREATED',
});

export const provider_columns = [
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
    field: 'software',
    headerName: 'Name',
    flex: 1,
    editable: false,
    sortable: false,
    filterable: false,
    hide: false,
    renderCell: function (params) {
      const provider =
        providerLogos[params.row.software?.toUpperCase()] ??
        providerLogos['LOCALMODE'];
      return (
        <>
          <img
            alt={params.row.software}
            style={{
              width: '4.0em',
              height: '2.5em',
            }}
            src={provider}
          />
          <Typography variant='body2' sx={{ marginLeft: 1 }}>
            <strong>{params.row.name}</strong>
          </Typography>
        </>
      );
    },
  },
  {
    field: 'provider-type',
    headerName: 'Type',
    flex: 1,
    maxWidth: 175,
    editable: false,
    sortable: false,
    filterable: false,
    renderCell: function (params) {
      const typeMap = {
        online: { path: 'static/online_store.svg', text: 'Online Store' },
        offline: { path: 'static/offline_store.svg', text: 'Offline Store' },
        file: { path: 'static/file_store.svg', text: 'File Store' },
      };

      let result = typeMap.file;
      let providerType = params?.row?.['provider-type'];

      if (providerType && typeof providerType === 'string') {
        providerType = providerType.toLocaleLowerCase();
        if (providerType.includes('online')) {
          result = typeMap.online;
        } else if (providerType.includes('offline')) {
          result = typeMap.offline;
        }
      }

      return (
        <>
          <img
            style={{
              width: '2em',
              height: '1.5em',
            }}
            src={result.path}
          />
          <Typography variant='body2' sx={{ marginLeft: 0.5 }}>
            {result.text}
          </Typography>
        </>
      );
    },
  },
  {
    field: 'tags',
    headerName: 'Tags',
    flex: 1,
    width: 350,
    editable: false,
    sortable: false,
    filterable: false,
    hide: true,
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
    field: 'sources',
    headerName: 'Owner',
    flex: 1,
    width: 450,
    editable: false,
    sortable: false,
    filterable: false,
    renderCell: function (params) {
      const [latestResource, numberOfResources] = getLatestSource(
        params.row?.sources
      );

      let latestOwner = '---';
      let count = 0;

      if (latestResource) {
        latestOwner = latestResource?.owner;
        count = numberOfResources;
      }

      return (
        <div style={{ display: 'flex' }}>
          <div style={{ marginTop: 10 }}>
            <UserBubbleSvg
              height='30'
              width='30'
              letter={latestOwner[0]?.toUpperCase()}
            />
          </div>
          <div>
            <div style={{ display: 'flex' }}>
              <Typography variant='body2' sx={{ marginLeft: 0.5 }}>
                {latestOwner}
              </Typography>
            </div>
            <div style={{ display: 'flex' }}>
              <Typography variant='body2' sx={{ marginLeft: 0.5 }}>
                # of Resources: {count}
              </Typography>
            </div>
          </div>
        </div>
      );
    },
  },
  {
    field: 'status',
    headerName: 'Status',
    flex: 1,
    width: 350,
    editable: false,
    sortable: false,
    filterable: false,
    renderCell: function (params) {
      const connectedFill = '#6DDE6A';
      let result = STATUS_COLORS.ERROR;
      if (params?.row?.status === PROVIDER_STATUS.READY) {
        result = STATUS_COLORS.READY;
      } else if (
        params?.row?.status === PROVIDER_STATUS.CREATED &&
        FILE_STORE_TYPES.includes(params?.row?.['provider-type'])
      ) {
        result = STATUS_COLORS.READY;
      }

      //connection
      const latestReady = getLatestReadySource(params?.row?.sources);
      const latestConnection = latestReady?.created
        ? new Date(latestReady.created)
        : '---';

      return (
        <div>
          <div style={{ display: 'flex' }}>
            <ConnectionSvg fill={result} height='20' width='20' />
            <Typography variant='body2' sx={{ marginLeft: 1 }}>
              Status: {result === STATUS_COLORS.READY ? 'Connected' : 'Disconnected'}
            </Typography>
          </div>
          <div style={{ display: 'flex' }}>
            <Typography variant='body2' sx={{ marginLeft: 1 }}>
              Last Connection: {latestConnection?.toLocaleString()}
            </Typography>
          </div>
        </div>
      );
    },
  },
];

function getLatestSource(sources = null) {
  let latestSource = null;
  let count = 0;

  if (!sources) {
    return [latestSource, count];
  }
  try {
    Object.keys(sources).forEach((key) => {
      sources[key]?.forEach((source) => {
        count++;
        if (
          !latestSource ||
          new Date(source.created) > new Date(latestSource.created)
        ) {
          latestSource = source;
        }
      });
    });
    return [latestSource, count];
  } catch (error) {
    console.error(error);
    return [latestSource, count];
  }
}

function getLatestReadySource(sources) {
  let latestSource = null;

  if (!sources) {
    return latestSource;
  }
  try {
    Object.keys(sources).forEach((key) => {
      sources[key]?.forEach((source) => {
        if (source.status === 'READY') {
          if (
            !latestSource ||
            new Date(source.created) > new Date(latestSource.created)
          ) {
            latestSource = source;
          }
        }
      });
    });
    return latestSource;
  } catch (error) {
    console.error(error);
    return null;
  }
}

const DEFAULT_FILTERS = Object.freeze({
  SearchTxt: '',
  Status: [], //'connected', 'disconnected'
  ProviderType: [], //'online', 'offline', 'file'
  pageSize: 10,
  offset: 0,
});

export const ProviderTable = () => {
  const [filters, setFilters] = useState({
    ...DEFAULT_FILTERS,
  });

  let router = useRouter();
  const [rows, setRows] = useState([]);
  const [loading, setIsLoading] = useState(false);
  const [totalRowCount, setTotalRowCount] = useState(0);

  const dataAPI = useDataAPI();

  useEffect(() => {
    const getResources = async () => {
      setIsLoading(true);
      try {
        let resp = await dataAPI.getProviders(filters);
        if (resp) {
          setRows(resp.data?.length ? resp.data : []);
          setTotalRowCount(resp.count);
        }
      } catch (error) {
        console.error('Failed to fetch providers:', error);
      } finally {
        setIsLoading(false);
      }
    };
    getResources();
  }, [filters]);

  const checkBoxFilterChange = (key, value) => {
    const updateFilters = { ...filters };
    const list = updateFilters[key];
    const index = list.indexOf(value);

    if (index === -1) {
      list.push(value);
    } else {
      list.splice(index, 1);
    }
    setFilters(updateFilters);
  };

  const onTextFieldEnter = useCallback(
    (value = '') => {
      const updateFilters = { ...filters, SearchTxt: value };
      setFilters(updateFilters);
    },
    [filters]
  );

  const handlePageChange = useCallback(
    (newPage) => {
      const updatedFilters = { ...filters, offset: newPage };
      setFilters(updatedFilters);
    },
    [filters]
  );

  const redirect = (name = '') => {
    router.push(`/providers/${name}`);
  };

  return (
    <>
      <MainContainer>
        <BaseFilterPanel onTextFieldEnter={onTextFieldEnter}>
          <FilterPanel
            filters={filters}
            onCheckBoxChange={checkBoxFilterChange}
          />
        </BaseFilterPanel>
        <GridContainer>
          <h3>{'Providers'}</h3>
          {loading ? (
            <div data-testid='loadingGrid'>
              <StyledDataGrid
                disableVirtualization
                aria-label={'Providers'}
                autoHeight
                density='compact'
                loading={loading}
                rows={[]}
                rowCount={0}
                columns={provider_columns}
                hideFooterSelectedRowCount
                disableColumnFilter
                disableColumnMenu
                disableColumnSelector
                paginationMode='server'
                rowsPerPageOptions={[filters.pageSize]}
              />
            </div>
          ) : (
            <StyledDataGrid
              disableVirtualization
              aria-label={'Provider'}
              rows={rows}
              columns={provider_columns}
              density='compact'
              rowHeight={80}
              hideFooterSelectedRowCount
              disableColumnFilter
              disableColumnMenu
              disableColumnSelector
              paginationMode='server'
              sortModel={[{ field: 'name', sort: 'asc' }]}
              onPageChange={(newPage) => {
                handlePageChange(newPage);
              }}
              components={{
                NoRowsOverlay: () => (
                  <NoDataMessage
                    type={'Provider'}
                    usingFilters={!isMatchingDefault(DEFAULT_FILTERS, filters)}
                  />
                ),
              }}
              rowCount={totalRowCount}
              page={filters.offset}
              pageSize={filters.pageSize}
              rowsPerPageOptions={[filters.pageSize]}
              onRowClick={(params, event) => {
                event?.preventDefault();
                if (params?.row?.name) {
                  redirect(params.row.name, params.row.variant);
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

export default ProviderTable;
