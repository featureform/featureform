import { Popover } from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import React, { useState } from 'react';
import TaskCard from './taskCard';

export default function TasksDataGrid({ taskList = [] }) {
  const [open, setOpen] = useState(false);
  const [content, setContent] = useState({});

  const handleRowSelect = (selectedRow) => {
    let foundTask = taskList?.find((q) => q.name === selectedRow.row.name);
    setContent(foundTask ?? {});
    setOpen((prev) => content !== selectedRow.row.name || !prev);
  };

  function handleClose() {
    setOpen(false);
  }

  const columns = [
    {
      field: 'id',
      headerName: 'TaskID',
      width: 100,
      editable: false,
      sortable: false,
      filterable: false,
      hide: true,
      hideable: false,
    },
    {
      field: 'name',
      headerName: 'Name',
      width: 200,
      editable: false,
      sortable: false,
      filterable: false,
    },
    {
      field: 'type',
      headerName: 'Type',
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
        <TaskCard handleClose={handleClose} searchId={content?.id ?? ''} />
      </Popover>
      <DataGrid
        sx={{ minWidth: 300, height: 475 }}
        onRowClick={handleRowSelect}
        density='compact'
        aria-label='Task Runs'
        rows={taskList ?? []}
        columns={columns}
        initialState={{
          pagination: { paginationModel: { page: 0, pageSize: 5 } },
        }}
        pageSize={10}
      ></DataGrid>
    </>
  );
}
