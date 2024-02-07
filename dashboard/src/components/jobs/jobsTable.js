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
  const handleRowSelect = () => {
    console.log('row has been selected');
  };
  return (
    <>
      <TableContainer component={Paper}>
        <Table sx={{ minWidth: 300 }} aria-label='Job Runs'>
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell align='right'>Type</TableCell>
              <TableCell align='right'>Provider</TableCell>
              <TableCell align='right'>Resource</TableCell>
              <TableCell align='right'>Variant</TableCell>
              <TableCell align='right'>Status</TableCell>
              <TableCell align='right'>Last Runtime</TableCell>
              <TableCell align='right'>Triggered By</TableCell>
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
                  <TableCell align='right'>{job.type}</TableCell>
                  <TableCell align='right'>{job.provider}</TableCell>
                  <TableCell align='right'>{job.resource}</TableCell>
                  <TableCell align='right'>{job.variant}</TableCell>
                  <TableCell align='right'>{job.status}</TableCell>
                  <TableCell align='right'>{job.lastRuntime}</TableCell>
                  <TableCell align='right'>{job.triggeredBy}</TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </TableContainer>
    </>
  );
}
