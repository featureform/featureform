import RemoveCircleOutlineIcon from '@mui/icons-material/RemoveCircleOutline';
import { Box, Button, Typography } from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import React from 'react';

export default function TriggerDetail({ handleClose }) {
  console.log('TRIGGER DETAL');

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
      width: 125,
      valueGetter: () => {
        return new Date()?.toLocaleString();
      },
    },
    {
      field: 'remove',
      headerName: 'Remove Resource',
      flex: 1,
      width: 125,
      editable: false,
      sortable: false,
      filterable: false,
      renderCell: function () {
        return <RemoveCircleOutlineIcon />;
      },
    },
  ];

  return (
    <>
      <Box sx={{ marginBottom: '2em' }}>
        <Typography>Trigger Type:</Typography>
        <Typography>Schedule:</Typography>
        <Typography>Owner:</Typography>
      </Box>
      <DataGrid
        density='compact'
        autoHeight
        aria-label='Other Runs'
        rows={[]}
        rowsPerPageOptions={[5]}
        columns={columns}
        initialState={{
          pagination: { paginationModel: { page: 0, pageSize: 5 } },
        }}
        pageSize={5}
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
          onClick={() => null}
          sx={{ margin: '0.5em', background: '#7A14E5' }}
        >
          Delete Trigger
        </Button>
      </Box>
    </>
  );
}
