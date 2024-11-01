// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { Popover } from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import React, { useState } from 'react';
import StatusChip from './statusChip';
import TaskRunCard from './taskRunCard';

export default function TaskRunDataGrid({
  taskRunList = [],
  setPage = () => null,
  count = 0,
  currentPage = 0,
}) {
  const [open, setOpen] = useState(false);
  const [content, setContent] = useState({ taskRun: { taskId: 0, runId: 0 } });

  const TASK_TYPE_MAP = {
    0: 'Resource Creation',
    1: 'Health Check',
    2: 'Metrics',
  };

  const isValidDate = (date) => {
    return date instanceof Date && !isNaN(date);
  };

  const timeSortedList = [...taskRunList].sort((a, b) => {
    const dateA = new Date(a.taskRun?.startTime);
    const dateB = new Date(b.taskRun?.startTime);

    if (isValidDate(dateA) && isValidDate(dateB)) {
      return dateB.getTime() - dateA.getTime();
    }

    //a date is invalid, do not modify the list
    console.warn('A task start time prop is invalid:', a?.taskRun, b?.taskRun);
    return 0;
  });

  const handleRowSelect = (selectedRow) => {
    let foundTaskRun = timeSortedList?.find(
      (q) =>
        q.taskRun.taskId === selectedRow.row.taskRun.taskId &&
        q.taskRun.runId === selectedRow.row.taskRun.runId
    );
    setContent(foundTaskRun ?? { taskRun: { taskId: 0, runId: 0 } });
    setOpen((prev) => content !== selectedRow.row.id || !prev);
  };

  function handleClose() {
    setOpen(false);
  }

  const columns = [
    {
      field: 'id',
      headerName: 'Id',
      flex: 1,
      width: 100,
      editable: false,
      sortable: false,
      filterable: false,
      hide: true,
    },
    {
      field: 'taskId',
      headerName: 'Task Id',
      flex: 0,
      width: 75,
      editable: false,
      sortable: false,
      filterable: false,
      hide: false,
      valueGetter: (params) => {
        return params?.row?.taskRun?.taskId;
      },
    },
    {
      field: 'runId',
      headerName: 'Run Id',
      flex: 0,
      width: 75,
      editable: false,
      sortable: false,
      filterable: false,
      hide: false,
      valueGetter: (params) => {
        return params?.row?.taskRun?.runId;
      },
    },
    {
      field: 'name',
      headerName: 'Task Name',
      flex: 1,
      minWidth: 250,
      editable: false,
      sortable: false,
      filterable: false,
      valueGetter: (params) => {
        return params?.row?.taskRun?.name;
      },
      hide: true,
    },
    {
      field: 'resource',
      headerName: 'Resource',
      flex: 1,
      minWidth: 150,
      editable: false,
      sortable: false,
      filterable: false,
      valueGetter: (params) => {
        return params?.row?.task?.target?.name;
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
        return params?.row?.task?.target?.variant;
      },
    },
    {
      field: 'jobType',
      headerName: 'Task Type',
      flex: 1,
      minWidth: 150,
      editable: false,
      sortable: false,
      filterable: false,
      valueGetter: (params) => {
        return TASK_TYPE_MAP[params?.row?.task?.type] ?? '';
      },
    },
    {
      field: 'status',
      headerName: 'Status',
      flex: 0,
      minWidth: 75,
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
      flex: 1,
      sortable: false,
      filterable: false,
      minWidth: 175,
      valueGetter: (params) => {
        return new Date(params?.row?.taskRun?.startTime)?.toLocaleString();
      },
    },
    {
      field: 'endTime',
      headerName: 'End Time',
      flex: 1,
      sortable: false,
      filterable: false,
      minWidth: 175,
      valueGetter: (params) => {
        const dateString = new Date(
          params?.row?.taskRun?.endTime
        )?.toLocaleString();
        return dateString.startsWith('12/31/1') ? '---' : dateString;
      },
    },
    {
      field: 'triggeredBy',
      headerName: 'Triggered By',
      flex: 1,
      width: 125,
      editable: false,
      valueGetter: (params) => {
        return params?.row?.taskRun?.trigger?.triggerName;
      },
      hide: true,
    },
  ];

  const mainPageSize = getPageSizeProp(timeSortedList?.length);

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
          taskId={content.taskRun.taskId}
          taskRunId={content.taskRun.runId}
        />
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
        rowsPerPageOptions={[15]}
        disableColumnMenu
        density='compact'
        aria-label='Task Runs'
        rows={timeSortedList ?? []}
        columns={columns}
        hideFooterSelectedRowCount
        rowCount={count}
        page={currentPage}
        paginationMode='server'
        onPageChange={(newPage) => {
          setPage(newPage);
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
  if (listLength >= 10) {
    pageSize = 15;
  } else if (listLength >= 5) {
    pageSize = 10;
  } else {
    pageSize = 5;
  }
  return pageSize;
}
