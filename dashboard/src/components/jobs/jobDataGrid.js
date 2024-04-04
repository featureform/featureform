import { Popover } from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import React, { useState } from 'react';
import JobCard from './jobCard';
import StatusChip from './statusChip';

export default function JobDataGrid({ jobList = [] }) {
  const DEFAULT_JOB = { job: { jobId: 0 } };
  const [open, setOpen] = useState(false);
  const [content, setContent] = useState({ ...DEFAULT_JOB });

  const JOB_TYPE_MAP = {
    0: 'Feature',
    1: 'Training',
  };

  const handleRowSelect = (selectedRow) => {
    let foundJob = jobList?.find(
      (q) => q.job.jobId === selectedRow.row.job.jobId
    );
    setContent(foundJob ?? { ...DEFAULT_JOB });
    setOpen((prev) => content !== selectedRow.row.id || !prev);
  };

  function handleClose() {
    setOpen(false);
  }

  const columns = [
    {
      field: 'jobId',
      headerName: 'Job ID',
      flex: 1,
      width: 50,
      editable: false,
      sortable: false,
      filterable: false,
      hide: false,
      valueGetter: (params) => {
        return params?.row?.job?.jobId;
      },
    },
    {
      field: 'name',
      headerName: 'Job Name',
      flex: 1,
      minWidth: 250,
      editable: false,
      sortable: false,
      filterable: false,
      valueGetter: (params) => {
        return params?.row?.job?.name;
      },
    },
    {
      field: 'variant',
      headerName: 'Variant',
      flex: 1,
      minWidth: 175,
      editable: false,
      sortable: false,
      filterable: false,
      valueGetter: (params) => {
        return params?.row?.job?.variant;
      },
    },
    {
      field: 'jobType',
      headerName: 'Job Type',
      flex: 1,
      minWidth: 150,
      editable: false,
      sortable: false,
      filterable: false,
      valueGetter: (params) => {
        return JOB_TYPE_MAP[params?.row?.job?.type] ?? '';
      },
    },
    {
      field: 'status',
      headerName: 'Status',
      flex: 1,
      minWidth: 75,
      editable: false,
      sortable: false,
      filterable: false,
      valueGetter: (params) => {
        return params?.row?.job?.status;
      },
      renderCell: function (params) {
        return <StatusChip status={params?.row?.job?.status} />;
      },
    },
    {
      field: 'lastRun',
      headerName: 'Last Run',
      flex: 1,
      sortable: false,
      filterable: false,
      minWidth: 175,

      valueGetter: (params) => {
        return new Date(params?.row?.job?.endTime)?.toLocaleString();
      },
    },
    {
      field: 'triggeredBy',
      headerName: 'Triggered By',
      flex: 1,
      width: 125,
      editable: false,
      valueGetter: (params) => {
        return params?.row?.job?.trigger?.triggerName;
      },
    },
  ];

  const mainPageSize = getPageSizeProp(jobList?.length);

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
        <JobCard handleClose={handleClose} jobId={content.job.jobId} />
      </Popover>
      <DataGrid
        autoHeight
        sx={{
          minWidth: 300,
          '& .MuiDataGrid-cell:focus': {
            outline: 'none',
          },
          '& .MuiDataGrid-row:hover': {
            cursor: 'pointer',
          },
          '& .MuiDataGrid-columnHeaderTitle': {
            fontWeight: 'bold',
          },
        }}
        onRowClick={handleRowSelect}
        rowsPerPageOptions={[5, 10, 15]}
        disableColumnMenu
        density='compact'
        aria-label='Jobs'
        rows={jobList ?? []}
        columns={columns}
        initialState={{
          pagination: { paginationModel: { page: 1, pageSize: mainPageSize } },
        }}
        pageSize={mainPageSize}
        getRowId={(row) => {
          return row.id;
        }}
      />
    </>
  );
}

export function getPageSizeProp(listLength = 0) {
  let pageSize = 5;
  if (listLength > 10) {
    pageSize = 15;
  } else if (listLength > 5) {
    pageSize = 10;
  } else {
    pageSize = 5;
  }
  return pageSize;
}
