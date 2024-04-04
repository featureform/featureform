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

export default function JobCard({ handleClose, jobId }) {
  const classes = useStyles();
  const dataAPI = useDataAPI();
  const [jobRecord, setJobRecord] = useState({});
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
        return 'Future Link';
      },
    },
  ];

  useEffect(async () => {
    let timeout = null;
    if (jobId && loading) {
      let data = await dataAPI.getJobDetails(jobId);
      setJobRecord(data);
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
  }, [jobId, loading]);

  const handleReloadRequest = () => {
    if (!loading) {
      setLoading(true);
    }
  };

  return (
    <Box className={classes.jobCardBox}>
      <Box style={{ float: 'right' }}>
        <Tooltip title='Refresh card' placement='bottom'>
          <IconButton variant='' size='large' onClick={handleReloadRequest}>
            {loading ? (
              <CircularProgress size={'.75em'} />
            ) : (
              <RefreshIcon data-testid='jobRefreshIcon' />
            )}
          </IconButton>
        </Tooltip>
        <IconButton variant='' size='large' onClick={() => handleClose()}>
          <CloseIcon />
        </IconButton>
      </Box>
      <Grid style={{ padding: 12 }} container>
        <Grid item xs={6} justifyContent='flex-start'>
          <Typography variant='h5'>{jobRecord?.name}</Typography>
        </Grid>
        <Grid item xs={6} justifyContent='center'>
          <Typography variant='h6'>
            Status: <StatusChip status={jobRecord?.status} />
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
            value={jobRecord?.logs?.join('\n') + '\n' + jobRecord?.error}
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
          <Typography variant='h6'>Task Runs</Typography>
        </Grid>
        <Grid item xs={12} justifyContent='flex-start'></Grid>
        <Grid item xs={12} justifyContent='center'>
          <DataGrid
            density='compact'
            aria-label='Task Runs'
            autoHeight
            sx={{
              '& .MuiDataGrid-cell:focus': {
                outline: 'none',
              },
              '& .MuiDataGrid-columnHeaderTitle': {
                fontWeight: 'bold',
              },
            }}
            rows={jobRecord?.taskRuns ?? []}
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
  );
}
