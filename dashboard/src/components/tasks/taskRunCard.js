import DoubleArrowIcon from '@mui/icons-material/DoubleArrow';
import RefreshIcon from '@mui/icons-material/Refresh';
import {
  Box,
  CircularProgress,
  Grid,
  IconButton,
  TextField,
  Typography,
} from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import React, { useEffect, useState } from 'react';
import { useDataAPI } from '../../hooks/dataAPI';
import StatusChip from './statusChip';
import { useStyles } from './styles';

export default function TaskRunCard({ handleClose, searchId }) {
  const classes = useStyles();
  const dataAPI = useDataAPI();
  const [taskRunRecord, setTaskRecord] = useState({});
  const [loading, setLoading] = useState(true);

  const columns = [
    {
      field: 'id',
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
      field: 'lastRunTime',
      headerName: 'Date/Time',
      sortable: false,
      filterable: false,
      width: 200,
      valueGetter: (params) => {
        return new Date(params?.row?.lastRunTime)?.toLocaleString();
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
    },
  ];

  useEffect(async () => {
    let timeout = null;
    if (searchId && loading) {
      let data = await dataAPI.getTaskRunDetails(searchId);
      setTaskRecord(data);
      timeout = setTimeout(() => {
        setLoading(false);
      }, 750);
    }
    return () => {
      if (timeout) {
        clearTimeout(timeout);
      }
    };
  }, [searchId, loading]);

  const handleReloadRequest = () => {
    if (!loading) {
      setLoading(true);
    }
  };

  return (
    <Box className={classes.taskCardBox}>
      <Box style={{ float: 'left' }}>
        <IconButton variant='' size='large' onClick={() => handleClose()}>
          <DoubleArrowIcon />
        </IconButton>
      </Box>
      <Box style={{ float: 'right' }}>
        <IconButton variant='' size='large' onClick={handleReloadRequest}>
          {loading ? <CircularProgress size={'.75em'} /> : <RefreshIcon />}
        </IconButton>
      </Box>
      <Grid style={{ padding: 12 }} container>
        <Grid item xs={6} justifyContent='flex-start'>
          <Typography variant='h5'>{taskRunRecord.name}</Typography>
        </Grid>
        <Grid item xs={6} justifyContent='center'>
          <Typography variant='h6'>
            Status: <StatusChip status={taskRunRecord.status} />
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
            value={taskRunRecord.logs}
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
            value={taskRunRecord.details}
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
            rows={taskRunRecord?.otherRuns ?? []}
            rowsPerPageOptions={[5]}
            columns={columns}
            initialState={{
              pagination: { paginationModel: { page: 0, pageSize: 5 } },
            }}
            pageSize={5}
          />
        </Grid>
      </Grid>
    </Box>
  );
}
