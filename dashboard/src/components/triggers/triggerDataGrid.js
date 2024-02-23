import RemoveCircleOutlineIcon from '@mui/icons-material/RemoveCircleOutline';
import { DataGrid } from '@mui/x-data-grid';
import React from 'react';

export default function TriggerDataGrid({ triggerList = [] }) {
  const columns = [
    {
      field: 'id',
      headerName: 'id',
      flex: 1,
      width: 100,
      editable: false,
      sortable: false,
      filterable: false,
      hide: true,
    },
    {
      field: 'name',
      headerName: 'Name',
      flex: 1,
      width: 200,
      editable: false,
      sortable: false,
      filterable: false,
    },
    {
      field: 'type',
      headerName: 'Type',
      flex: 1,
      width: 175,
      editable: false,
      sortable: false,
      filterable: false,
    },
    {
      field: 'schedule',
      headerName: 'Schedule',
      flex: 1,
      width: 175,
      editable: false,
      sortable: false,
      filterable: false,
    },
    {
      field: 'detail',
      headerName: 'Trigger Detail',
      flex: 1,
      width: 175,
      editable: false,
      sortable: false,
      filterable: false,
    },
    {
      field: 'remove',
      headerName: 'Remove Trigger',
      flex: 1,
      width: 125,
      editable: false,
      sortable: false,
      filterable: false,
      renderCell: function (params) {
        return <RemoveCircleOutlineIcon />;
      },
    },
  ];

  return (
    <>
      <DataGrid
        sx={{ minWidth: 300, height: 650 }}
        density='compact'
        aria-label='Triggers'
        rows={triggerList ?? []}
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
