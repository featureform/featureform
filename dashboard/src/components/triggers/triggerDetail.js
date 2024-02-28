import RemoveCircleOutlineIcon from '@mui/icons-material/RemoveCircleOutline';
import {
  Alert,
  Box,
  Button,
  IconButton,
  Tooltip,
  Typography,
} from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import React, { useState } from 'react';

export default function TriggerDetail({
  details = {},
  handleClose,
  handleDelete,
  handleDeleteResource,
  rowDelete = false,
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

  const PRE_DELETE = 'Delete Trigger';
  const CONFIRM_DELETE = 'Confirm, Delete!';
  const [userConfirm, setUserConfirm] = useState(rowDelete);

  // todox: track this in state?
  const isDeleteDisabled = () => {
    let result = true;
    if (
      details?.resources?.length === undefined ||
      details?.resources?.length === 0
    ) {
      result = false;
    } else {
      result = true;
    }
    return result;
  };

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
      <Box sx={{ marginTop: '1em' }}>
        {rowDelete && isDeleteDisabled() ? (
          <Alert severity='warning'>
            To delete the trigger, first remove all its resources.
          </Alert>
        ) : (
          <></>
        )}
      </Box>
      <Box sx={{ marginTop: '1em' }} display={'flex'} justifyContent={'end'}>
        <Button
          variant='contained'
          onClick={handleClose}
          sx={{ margin: '0.5em', background: '#FFFFFF', color: '#000000' }}
        >
          Cancel
        </Button>
        <Tooltip
          placement='top'
          title={
            isDeleteDisabled()
              ? 'To delete the trigger, remove all resources first.'
              : ''
          }
          disabled
        >
          <span>
            <Button
              variant='contained'
              disabled={isDeleteDisabled()}
              onClick={() => {
                if (!isDeleteDisabled()) {
                  if (userConfirm) {
                    handleDelete?.(details?.trigger?.id);
                  } else {
                    setUserConfirm(true);
                  }
                }
              }}
              sx={{
                margin: '0.5em',
                background: '#DA1E28',
              }}
            >
              {userConfirm ? CONFIRM_DELETE : PRE_DELETE}
            </Button>
          </span>
        </Tooltip>
      </Box>
    </>
  );
}
