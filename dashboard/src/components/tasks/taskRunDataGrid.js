import { Popover } from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import React, { useState } from 'react';
import StatusChip from './statusChip';
import TaskRunCard from './taskRunCard';

export default function TaskRunDataGrid({ taskRunList = [] }) {
  const [open, setOpen] = useState(false);
  const [content, setContent] = useState({});

  const handleRowSelect = (selectedRow) => {
    let foundTaskItem = taskRunList?.find(
      (q) => q.taskRun.runId === selectedRow.row.taskRun.runId
    );
    setContent(foundTaskItem.taskRun ?? {});
    setOpen((prev) => content !== selectedRow.row.taskRun.name || !prev);
  };

  function handleClose() {
    setOpen(false);
  }

  const columns = [
    {
      field: 'runId',
      headerName: 'Run Id',
      width: 100,
      editable: false,
      sortable: false,
      filterable: false,
      hide: true,
    },
    {
      field: 'name',
      headerName: 'Job Run Name',
      width: 200,
      editable: false,
      sortable: false,
      filterable: false,
      valueGetter: (params) => {
        return params?.row?.taskRun?.name;
      },
    },

    {
      field: 'triggerType',
      headerName: 'Job Type',
      width: 175,
      editable: false,
      sortable: false,
      filterable: false,
      valueGetter: (params) => {
        return params?.row?.taskRun?.triggerType;
      },
    },
    {
      field: 'status',
      headerName: 'Status',
      width: 150,
      editable: false,
      sortable: false,
      filterable: false,
      valueGetter: (params) => {
        return params?.row?.taskRun?.status;
      },
      renderCell: function (params) {
        return <StatusChip status={params?.row?.taskRun?.status} />;
      },
    },
    {
      field: 'startTime',
      headerName: 'Start Time',
      sortable: false,
      filterable: false,
      width: 200,
      valueGetter: (params) => {
        return new Date(params?.row?.taskRun?.startTime)?.toLocaleString();
      },
    },
    {
      field: 'endTime',
      headerName: 'End Time',
      sortable: false,
      filterable: false,
      width: 200,
      valueGetter: (params) => {
        return new Date(params?.row?.taskRun?.endTime)?.toLocaleString();
      },
    },
    {
      field: 'triggeredBy',
      headerName: 'Triggered By',
      width: 150,
      editable: false,
      valueGetter: (params) => {
        return params?.row?.taskRun?.trigger?.triggerName;
      },
    },
  ];

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
        <TaskRunCard
          handleClose={handleClose}
          searchId={content?.runId ?? ''}
        />
      </Popover>
      <DataGrid
        sx={{ minWidth: 300, height: 650 }}
        onRowClick={handleRowSelect}
        density='compact'
        aria-label='Task Runs'
        rows={taskRunList ?? []}
        rowsPerPageOptions={[15]}
        columns={columns}
        initialState={{
          pagination: { paginationModel: { page: 1, pageSize: 15 } },
        }}
        pageSize={15}
        getRowId={(row) => {
          return row.taskRun.runId;
        }}
      />
    </>
  );
}
