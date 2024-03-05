import CloseIcon from '@mui/icons-material/Close';
import RefreshIcon from '@mui/icons-material/Refresh';
import {
  Box,
  CircularProgress,
  Grid,
  IconButton,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import React, { useEffect, useState } from 'react';
import { useDataAPI } from '../../hooks/dataAPI';
import StatusChip from './statusChip';
import { useStyles } from './styles';

export default function TaskRunCard({ handleClose, taskId, taskRunId }) {
  const classes = useStyles();
  const dataAPI = useDataAPI();
  const [taskRunRecord, setTaskRecord] = useState({});
  const [loading, setLoading] = useState(true);
  console.log("Opened tray", taskId, taskRunId)
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
      editable: false,
      sortable: false,
      filterable: false,
    },
    {
      field: 'link',
      headerName: 'Link',
      width: 250,
      editable: false,
      sortable: false,
      filterable: false,
      valueGetter: () => {
        return 'Future Link';
      },
    },
  ];

  useEffect(async () => {
    let timeout = null;
    if (taskId && taskRunId && loading) {
      let data = await dataAPI.getTaskRunDetails(taskId, taskRunId);
      setTaskRecord(data);
      const timeout = setTimeout(() => {
        setLoading(false);
      }, 750);
      return () => {
        if (timeout) {
          clearTimeout(timeout);
        }
      };
    }
    return () => {
      if (timeout) {
        clearTimeout(timeout);
      }
    };
  }, [taskId, taskRunId, loading]);

  const handleReloadRequest = () => {
    if (!loading) {
      setLoading(true);
    }
  };

  return (
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
      <Grid style={{ padding: 12 }} container>
        <Grid item xs={6} justifyContent='flex-start'>
          <Typography variant='h5'>{taskRunRecord?.taskRun?.name}</Typography>
        </Grid>
        <Grid item xs={6} justifyContent='center'>
          <Typography variant='h6'>
            Status: <StatusChip status={taskRunRecord?.taskRun?.status} />
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
            value={taskRunRecord?.taskRun?.logs.join('\n') + '\n' + taskRunRecord?.taskRun?.error}
            multiline
            minRows={3}
          ></TextField>
        </Grid>
        <Grid item xs={12} justifyContent='center' style={{ paddingTop: 20 }}>
          <Typography variant='h6'>Task Run Details</Typography>
        </Grid>
        <Grid item xs={12} justifyContent='center'>
          <TextField
            style={{ width: '100%' }}
            variant='filled'
            disabled
            value={'Todox: Need to fill'}
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
            }}
            rows={taskRunRecord?.otherRuns ?? []}
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
  );
}
