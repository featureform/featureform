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
import TaskCard from './taskCard';

export default function TasksTable({ taskList = [] }) {
  const [open, setOpen] = useState(false);
  const [content, setContent] = useState({});
  const headerRef = useRef();

  const handleRowSelect = (taskName) => {
    let foundTask = taskList.find((q) => q.name === taskName);
    setContent(foundTask ?? {});
    setOpen((prev) => content !== taskName || !prev);
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
        <TaskCard taskRecord={content} />
      </Popover>
      <TableContainer component={Paper}>
        <Table sx={{ minWidth: 300 }} aria-label='Task Runs'>
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
            {taskList?.map((task, index) => {
              return (
                <TableRow
                  key={index}
                  onClick={() => handleRowSelect(task.name)}
                  style={{ cursor: 'pointer' }}
                  hover
                >
                  <TableCell>{task.name}</TableCell>
                  <TableCell align='right'>{task.type}</TableCell>
                  <TableCell align='right'>{task.provider}</TableCell>
                  <TableCell align='right'>{task.resource}</TableCell>
                  <TableCell align='right'>{task.variant}</TableCell>
                  <TableCell align='right'>{task.status}</TableCell>
                  <TableCell align='right'>
                    {new Date(task.lastRunTime)?.toLocaleString()}
                  </TableCell>
                  <TableCell align='right'>{task.triggeredBy}</TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </TableContainer>
    </>
  );
}
