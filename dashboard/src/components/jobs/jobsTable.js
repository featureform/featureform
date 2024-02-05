import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from '@mui/material';
import React from 'react';

export default function JobsTable({ jobsList = [] }) {
  let dummyJob = {
    name: 'agg1',
    type: 'Source',
    provider: 'Spark',
    resource: 'perc_balance',
    variant: 'v1',
    status: 'Pending',
    lastRuntime: '2024-06-15',
    triggeredBy: 'On Apply',
  };
  jobsList.push(dummyJob, dummyJob, dummyJob, dummyJob, dummyJob);

  const handleRowSelect = (jobName) => {
    console.log('clicked on jobs row', jobName);
  };

  return (
    <>
      <TableContainer component={Paper}>
        <Table sx={{ minWidth: 300 }} aria-label='Job Runs'>
          <TableHead>
            <TableRow>
              <TableCell>Job Run Name</TableCell>
              <TableCell>Job Type</TableCell>
              <TableCell>Provider</TableCell>
              <TableCell>Resource</TableCell>
              <TableCell>Variant</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Last Runtime</TableCell>
              <TableCell>Triggered By</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {jobsList?.map((job, index) => {
              return (
                <TableRow
                  key={index}
                  onClick={() => handleRowSelect(job.name)}
                  style={{ cursor: 'pointer' }}
                  hover
                >
                  <TableCell>{job.name}</TableCell>
                  <TableCell>{job.type}</TableCell>
                  <TableCell>{job.provider}</TableCell>
                  <TableCell>{job.resource}</TableCell>
                  <TableCell>{job.variant}</TableCell>
                  <TableCell>{job.status}</TableCell>
                  <TableCell>{job.lastRuntime}</TableCell>
                  <TableCell>{job.triggeredBy}</TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </TableContainer>
    </>
  );
}
