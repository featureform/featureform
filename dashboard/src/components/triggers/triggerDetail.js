import RemoveCircleOutlineIcon from '@mui/icons-material/RemoveCircleOutline';
import { Box, Button, IconButton, Typography } from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import React from 'react';

export default function TriggerDetail({
  details = {},
  handleClose,
  handleDelete,
  handleDeleteResource,
}) {
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
      field: 'resource',
      headerName: 'Resource',
      flex: 1,
      width: 125,
      editable: false,
      sortable: false,
      filterable: false,
    },
    {
      field: 'variant',
      headerName: 'variant',
      flex: 1,
      width: 125,
      editable: false,
      sortable: false,
      filterable: false,
    },
    {
      field: 'lastRun',
      headerName: 'Last Run',
      flex: 1,
      sortable: false,
      filterable: false,
      width: 150,
      valueGetter: () => {
        return new Date()?.toLocaleString();
      },
    },
    {
      field: 'remove',
      headerName: 'Remove',
      flex: 0,
      width: 100,
      editable: false,
      sortable: false,
      filterable: false,
      renderCell: function (params) {
        return (
          <IconButton
            onClick={(e) =>
              handleDeleteResource?.(e, details?.trigger?.id, params?.id)
            }
          >
            <RemoveCircleOutlineIcon fontSize='large' />
          </IconButton>
        );
      },
    },
  ];

  return (
    <>
      <Box sx={{ marginBottom: '2em' }}>
        <Typography>Trigger Type: {details?.trigger?.type}</Typography>
        <Typography>Schedule: {details?.trigger?.schedule}</Typography>
        <Typography>Owner: {details?.owner}</Typography>
      </Box>
      <DataGrid
        density='compact'
        autoHeight
        sx={{
          '& .MuiDataGrid-cell:focus': {
            outline: 'none',
          },
        }}
        aria-label='Other Runs'
        rows={details?.resources ?? []}
        rowsPerPageOptions={[5]}
        columns={columns}
        initialState={{
          pagination: { paginationModel: { page: 0, pageSize: 5 } },
        }}
        pageSize={5}
        getRowId={(row) => row.resourceId}
      />
      <Box sx={{ marginTop: '1em' }} display={'flex'} justifyContent={'end'}>
        <Button
          variant='contained'
          onClick={handleClose}
          sx={{ margin: '0.5em', background: '#FFFFFF', color: '#000000' }}
        >
          Cancel
        </Button>
        <Button
          variant='contained'
          onClick={() => handleDelete?.(details?.trigger?.id)}
          sx={{ margin: '0.5em', background: '#7A14E5' }}
        >
          Delete Trigger
        </Button>
      </Box>
    </>
  );
}
