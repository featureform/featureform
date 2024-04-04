import { Box, Typography } from '@mui/material';
import LinearProgress from '@mui/material/LinearProgress';
import { makeStyles } from '@mui/styles';
import React from 'react';
import { JOB_TYPE_MAP, LINEAR_STATUS_MAP } from './jobDataGrid';
import StatusChip from './statusChip';

const useStyles = makeStyles((theme) => ({
  resourceItem: {
    paddingBottom: theme.spacing(1),
    display: 'flex',
  },
  typeTitle: {
    paddingRight: theme.spacing(1),
    fontWeight: 'bold',
  },
}));

export default function JobData({ jobRecord = {} }) {
  const classes = useStyles();
  return (
    <>
      <Box>
        <Box className={classes.resourceItem}>
          <Typography variant='body1' className={classes.typeTitle}>
            Status:
          </Typography>
          <StatusChip status={jobRecord?.job?.status}></StatusChip>
        </Box>
        <Box className={classes.resourceItem}>
          <Typography variant='body1' className={classes.typeTitle}>
            Last Run:
          </Typography>
          {new Date(jobRecord?.job?.lastRun)?.toLocaleString()}
        </Box>
        <Box className={classes.resourceItem}>
          <Typography variant='body1' className={classes.typeTitle}>
            Job Type:
          </Typography>
          {JOB_TYPE_MAP[jobRecord?.job?.type] ?? ''}
        </Box>
        <Box className={classes.resourceItem}>
          <Typography variant='body1' className={classes.typeTitle}>
            Progress:
          </Typography>
        </Box>
        <LinearProgress
          color={LINEAR_STATUS_MAP[jobRecord?.job?.status]}
          variant={'determinate'}
          value={jobRecord?.job?.progress ?? 0}
        />
        <Box className={classes.resourceItem} sx={{ paddingTop: 1 }}>
          <Typography variant='body1' className={classes.typeTitle}>
            Duration:
          </Typography>
          ?
        </Box>
        <Box className={classes.resourceItem}>
          <Typography variant='body1' className={classes.typeTitle}>
            Triggered By:
          </Typography>
          {jobRecord?.job?.triggeredBy}
        </Box>
        <Box className={classes.resourceItem}>
          <Typography variant='body1' className={classes.typeTitle}>
            Owner:
          </Typography>
          ?
        </Box>
      </Box>
    </>
  );
}
