import { Popover } from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import React, { useState } from 'react';
import StatusChip from './statusChip';
import TaskRunCard from './taskRunCard';

export default function TaskRunDataGrid({ taskRunList = [] }) {
  const [open, setOpen] = useState(false);
  const [content, setContent] = useState({});

  const handleRowSelect = (selectedRow) => {
    let foundTaskRun = taskRunList?.find((q) => q.id === selectedRow.row.id);
    setContent(foundTaskRun ?? {});
    setOpen((prev) => content !== selectedRow.row.name || !prev);
  };

  function handleClose() {
    setOpen(false);
  }

  const columns = [
    {
      field: 'id',
      headerName: 'Run Id',
      width: 100,
      editable: false,
      sortable: false,
      filterable: false,
      hide: true,
    },
    {
      field: 'name',
      headerName: 'Job Name',
      width: 200,
      editable: false,
      sortable: false,
      filterable: false,
    },
    {
      field: 'type',
      headerName: 'Job Type',
      width: 150,
      editable: false,
      sortable: false,
      filterable: false,
    },
    {
      field: 'provider',
      headerName: 'Provider',
      width: 200,
      editable: false,
      sortable: false,
      filterable: false,
    },
    {
      field: 'resource',
      headerName: 'Resource',
      width: 200,
      editable: false,
      sortable: false,
      filterable: false,
    },
    {
      field: 'status',
      headerName: 'Status',
      width: 150,
      editable: false,
      sortable: false,
      filterable: false,
      renderCell: function (params) {
        return <StatusChip status={params?.row?.status} />;
      },
    },
    {
      field: 'lastRunTime',
      headerName: 'Last Run',
      description: 'This column has a value getter and is not sortable.',
      sortable: false,
      filterable: false,
      width: 200,
      valueGetter: (params) => {
        return new Date(params?.row?.lastRunTime)?.toLocaleString();
      },
    },
    {
      field: 'triggeredBy',
      headerName: 'Triggered By',
      width: 150,
      editable: false,
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
        <TaskRunCard handleClose={handleClose} searchId={content?.id ?? ''} />
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
      />
    </>
  );
}
