import CloseIcon from '@mui/icons-material/Close';
import RefreshIcon from '@mui/icons-material/Refresh';
import {
  Box,
  CircularProgress,
  Grid,
  IconButton,
  Tooltip,
  Typography,
} from '@mui/material';
import { makeStyles } from '@mui/styles';
import { DataGrid } from '@mui/x-data-grid';
import React, { useEffect, useState } from 'react';
import { useDataAPI } from '../../hooks/dataAPI';
import JobData from './jobData';
import StatusChip from './statusChip';

const useStyles = makeStyles((theme) => ({
  jobData: {
    padding: theme.spacing(1),
    borderRadius: '16px',
    border: `2px solid ${theme.palette.border.main}`,
  },
}));

export default function JobCard({ handleClose, jobId }) {
  const classes = useStyles();
  const dataAPI = useDataAPI();
  const [jobRecord, setJobRecord] = useState({});
  const [loading, setLoading] = useState(true);
  const columns = [
    {
      field: 'id',
      headerName: 'id',
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
      headerName: 'Last Run Time',
      sortable: false,
      filterable: false,
      width: 200,
      valueGetter: (params) => {
        return new Date(params?.row?.lastRun)?.toLocaleString();
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
      field: 'name',
      headerName: 'Name',
      width: 250,
      editable: false,
      sortable: false,
      filterable: false,
    },
  ];

  useEffect(async () => {
    let timeout = null;
    if (jobId && loading) {
      let data = await dataAPI.getJobDetails(jobId);
      console.log('loaded');
      console.log(data);
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
    <Box>
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
        <Grid
          sx={{ paddingBottom: '1.5em' }}
          item
          xs={6}
          justifyContent='flex-start'
        >
          <Typography variant='h6'>
            Job Run: <strong>{jobRecord?.job?.name}</strong>
          </Typography>
        </Grid>
        <Grid item xs={6} justifyContent='center'></Grid>
        <Grid
          className={classes.jobData}
          item
          xs={8}
          justifyContent='flex-start'
        >
          <JobData jobRecord={jobRecord} />
        </Grid>
        <Grid
          item
          xs={12}
          justifyContent='flex-start'
          style={{ paddingTop: 20 }}
        >
          <Typography variant='h6'>Task Runs</Typography>
        </Grid>
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
            rows={jobRecord?.jobTaskRuns ?? []}
            disableColumnMenu
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
