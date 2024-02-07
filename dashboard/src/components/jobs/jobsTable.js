import {
  Paper,
  Popover,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from '@mui/material';
import React, { useRef, useState } from 'react';
import JobCard from './jobCard';

export default function JobsTable({ jobsList = [] }) {
  const [anchorEl, setAnchorEl] = useState(null);
  const [open, setOpen] = useState(false);
  const [content, setContent] = useState({});
  const headerRef = useRef();

  const handleRowSelect = (jobName, event) => {
    setAnchorEl(headerRef.currentTarget);
    let foundJob = jobsList.find((q) => q.name === jobName);
    setContent(foundJob ?? {});
    setOpen((prev) => content !== jobName || !prev);
  };

  const handleClose = () => {
    setOpen(false);
  };

  return (
    <>
      <Popover
        open={open}
        anchorReference='anchorPosition'
        anchorPosition={{ top: 250, left: Number.MAX_SAFE_INTEGER }}
        onClose={handleClose}
        anchorOrigin={{
          vertical: 'top',
          horizontal: 'right',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'right',
        }}
      >
        <JobCard jobRecord={content} />
      </Popover>
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
              <TableCell align='right' ref={headerRef}>
                Triggered By
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {jobsList?.map((job, index) => {
              return (
                <TableRow
                  key={index}
                  onClick={(event) => handleRowSelect(job.name, event)}
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
