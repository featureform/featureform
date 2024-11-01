// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import CloseIcon from '@mui/icons-material/Close';
import RefreshIcon from '@mui/icons-material/Refresh';
import {
  Box,
  CircularProgress,
  Grid,
  IconButton,
  Link,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import { styled } from '@mui/system';
import { DataGrid } from '@mui/x-data-grid';
import { useRouter } from 'next/router';
import React, { useEffect, useState } from 'react';
import Resource from '../../api/resources/Resource';
import { searchTypeMap } from '../../components/searchresults/SearchTable';
import { useDataAPI } from '../../hooks/dataAPI';
import { WHITE } from '../../styles/theme';
import StatusChip from './statusChip';

const DEFAULT_FILL = '#B3B5DC';

const PREFIX = 'TaskRunCard';

const classes = {
  taskCardBox: `${PREFIX}-taskCardBox`,
  inputRow: `${PREFIX}-inputRow`,
  activeButton: `${PREFIX}-activeButton`,
  activeChip: `${PREFIX}-activeChip`,
  inactiveChip: `${PREFIX}-inactiveChip`,
  inactiveButton: `${PREFIX}-inactiveButton`,
  buttonText: `${PREFIX}-buttonText`,
  filterInput: `${PREFIX}-filterInput`,
  search: `${PREFIX}-search`,
};

const Root = styled('div')(() => ({
  [`& .${classes.taskCardBox}`]: {
    height: 750,
    width: 700,
  },
  [`& .${classes.inputRow}`]: {
    paddingBottom: 10,
    '& button': {
      height: 40,
    },
  },
  [`& .${classes.activeButton}`]: {
    color: '#000000',
    fontWeight: 'bold',
  },
  [`& .${classes.activeChip}`]: {
    color: WHITE,
    background: '#7A14E5',
    height: 20,
    width: 25,
    fontSize: 11,
  },
  [`& .${classes.inactiveChip}`]: {
    color: WHITE,
    background: '#BFBFBF',
    height: 20,
    width: 25,
    fontSize: 11,
  },
  [`& .${classes.inactiveButton}`]: {
    color: '#000000',
    background: WHITE,
  },
  [`& .${classes.buttonText}`]: {
    textTransform: 'none',
    paddingRight: 10,
  },
  [`& .${classes.filterInput}`]: {
    minWidth: 225,
    height: 40,
  },
  [`& .${classes.search}`]: {
    background: '#101205',
    width: 350,
    '& label': {
      color: DEFAULT_FILL,
    },
    '& label.Mui-focused': {
      color: DEFAULT_FILL,
    },
    '& .MuiOutlinedInput-root': {
      color: DEFAULT_FILL,
      borderColor: DEFAULT_FILL,
      '& fieldset': {
        borderColor: DEFAULT_FILL,
      },
      '&.Mui-focused fieldset': {
        borderColor: DEFAULT_FILL,
      },
    },
  },
}));

export default function TaskRunCard({ handleClose, taskId, taskRunId }) {
  const dataAPI = useDataAPI();
  const router = useRouter();
  const [taskRunRecord, setTaskRunRecord] = useState({});
  const [loading, setLoading] = useState(true);

  const columns = [
    {
      field: 'runId',
      headerName: 'runId',
      width: 1,
      editable: false,
      sortable: false,
      filterable: false,
      hide: true,
      hideable: false,
      display: false,
    },
    {
      field: 'startTime',
      headerName: 'Start Time',
      sortable: false,
      filterable: false,
      width: 200,
      valueGetter: (params) => {
        return new Date(params?.row?.startTime)?.toLocaleString();
      },
    },
    {
      field: 'status',
      headerName: 'Status',
      width: 200,
      align: 'right',
      editable: false,
      sortable: false,
      filterable: false,
      renderCell: function (params) {
        return <StatusChip status={params?.row?.status} />;
      },
    },
    {
      field: 'link',
      headerName: 'Link',
      width: 250,
      editable: false,
      sortable: false,
      filterable: false,
      valueGetter: () => {
        return '';
      },
    },
  ];

  const handleResourceLinkClick = (taskRunRecord = {}) => {
    const name = taskRunRecord?.taskRun?.target?.name;
    const variant = taskRunRecord?.taskRun?.target?.variant;
    const resourceType =
      Resource[searchTypeMap[taskRunRecord?.taskRun?.target?.type]];
    if (name && variant && resourceType) {
      const resourceUrl = `${resourceType.urlPath}/${name}?variant=${variant}`;
      router.push(resourceUrl);
    } else {
      console.warn(
        `handleResourceClick() could not create resource link. name(${name}) variant(${variant}) resourceType(${resourceType?.urlPath})`
      );
    }
  };

  useEffect(() => {
    let timeout = null;
    const fetchData = async () => {
      if (taskId && taskRunId && loading) {
        let data = await dataAPI.getTaskRunDetails(taskId, taskRunId);
        setTaskRunRecord(data);
        timeout = setTimeout(() => {
          setLoading(false);
        }, 750);
      }
    };

    fetchData();

    return () => {
      if (timeout) {
        clearTimeout(timeout);
      }
    };
  }, [taskId, taskRunId, loading, dataAPI]);

  const handleReloadRequest = () => {
    if (!loading) {
      setLoading(true);
    }
  };

  const getLogs = (taskRun = {}) => {
    let result = '';
    if (taskRun?.taskRun?.logs) {
      result += taskRun.taskRun.logs.join('\n');
    }
    if (taskRun?.taskRun?.error) result += '\n' + taskRun.taskRun.error;
    return result;
  };

  return (
    <Root>
      <Box className={classes.taskCardBox}>
        <Box style={{ float: 'right' }}>
          <Tooltip title='Refresh card' placement='bottom'>
            <IconButton variant='' size='large' onClick={handleReloadRequest}>
              {loading ? (
                <CircularProgress size={'.75em'} />
              ) : (
                <RefreshIcon data-testid='taskRunRefreshIcon' />
              )}
            </IconButton>
          </Tooltip>
          <IconButton variant='' size='large' onClick={() => handleClose()}>
            <CloseIcon />
          </IconButton>
        </Box>
        <Grid style={{ padding: 10 }} container>
          <Grid item xs={7} justifyContent='flex-start'>
            <Typography variant='h5'>
              <Link
                style={{ cursor: 'pointer' }}
                onClick={() => handleResourceLinkClick(taskRunRecord)}
              >
                {taskRunRecord?.taskRun?.name ?? ''}
              </Link>
            </Typography>
          </Grid>
          <Grid item xs={4} justifyContent='center'>
            <Typography variant='h6'>
              <strong>Status:</strong>{' '}
              <StatusChip status={taskRunRecord?.taskRun?.status ?? ''} />
            </Typography>
          </Grid>
          <Grid
            item
            xs={12}
            justifyContent='flex-start'
            style={{ paddingTop: 20 }}
          >
            <Typography variant='h6'>Logs/Errors</Typography>
          </Grid>
          <Grid item xs={12} justifyContent='flex-start'>
            <TextField
              style={{ width: '100%' }}
              variant='filled'
              disabled
              value={getLogs(taskRunRecord)}
              multiline
              minRows={3}
            ></TextField>
          </Grid>
          <Grid
            item
            xs={12}
            justifyContent='flex-start'
            style={{ paddingTop: 20 }}
          >
            <Typography variant='h6'>Other Runs</Typography>
          </Grid>
          <Grid item xs={12} justifyContent='flex-start'></Grid>
          <Grid item xs={12} justifyContent='center'>
            <DataGrid
              density='compact'
              autoHeight
              aria-label='Other Runs'
              sx={{
                '& .MuiDataGrid-cell:focus': {
                  outline: 'none',
                },
                '& .MuiDataGrid-columnHeaderTitle': {
                  fontWeight: 'bold',
                },
              }}
              rows={taskRunRecord?.otherRuns ?? []}
              disableColumnMenu
              rowsPerPageOptions={[5]}
              columns={columns}
              initialState={{
                pagination: { paginationModel: { page: 0, pageSize: 5 } },
              }}
              pageSize={5}
              getRowId={(row) => row.runId}
            />
          </Grid>
        </Grid>
      </Box>
    </Root>
  );
}
