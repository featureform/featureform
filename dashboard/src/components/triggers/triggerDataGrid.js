import AddIcon from '@mui/icons-material/Add';
import RemoveCircleOutlineIcon from '@mui/icons-material/RemoveCircleOutline';
import { Button, IconButton, Popover } from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import React, { useState } from 'react';
import { useDataAPI } from '../../hooks/dataAPI';
import NewTrigger from './newTrigger';
import TriggerDialog from './triggerDialog';

export default function TriggerDataGrid({ triggerList = [], refresh }) {
  const [openNew, setOpenNew] = useState(false);
  const [openDialog, setOpenDialog] = useState(false);
  const [dialogTriggerId, setDialogTriggerId] = useState();
  const dataAPI = useDataAPI();
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
      field: 'delete',
      flex: 0,
      headerName: 'Delete',
      width: 100,
      align: 'center',
      editable: false,
      sortable: false,
      filterable: false,
      renderCell: function (params) {
        return (
          <IconButton onClick={(e) => deleteRow(e, params)}>
            <RemoveCircleOutlineIcon fontSize='large' />
          </IconButton>
        );
      },
    },
  ];

  const handleDelete = async (id = '') => {
    if (id) {
      const data = await dataAPI.deleteTrigger(id);
      if (data === true) {
        refresh?.();
        setOpenDialog(false);
      }
    }
  };

  const handleDeleteResource = async (e, triggerId, resourceId) => {
    e.stopPropagation();
    e.preventDefault();
    if (resourceId && triggerId) {
      await dataAPI.deleteTriggerResource(triggerId, resourceId);
      refresh?.();
    }
  };

  async function deleteRow(e, row) {
    e.stopPropagation();
    e.preventDefault();
    await handleDelete(row.id);
  }

  const handleRowSelect = (selectedRow) => {
    setOpenDialog(true);
    setDialogTriggerId(selectedRow?.row?.id);
  };

  const handleNewTrigger = () => {
    if (!openNew) {
      setOpenNew(true);
    }
  };

  function handleClose() {
    setOpenNew(false);
    refresh?.();
  }

  const handleCloseDialog = () => {
    setOpenDialog(false);
  };

  return (
    <>
      <DataGrid
        onRowClick={handleRowSelect}
        density='compact'
        sx={{
          minWidth: 300,
          height: 650,
          '& .MuiDataGrid-cell:focus': {
            outline: 'none',
          },
          '& .MuiDataGrid-row:hover': {
            cursor: 'pointer',
          },
        }}
        aria-label='Triggers'
        rows={triggerList ?? []}
        rowsPerPageOptions={[15]}
        columns={columns}
        initialState={{
          pagination: { paginationModel: { page: 1, pageSize: 15 } },
        }}
        pageSize={15}
      />
      <Popover
        open={openNew}
        anchorReference='anchorPosition'
        anchorPosition={{ top: 500, left: 275 }}
        onClose={handleClose}
        anchorOrigin={{
          vertical: 'top',
          horizontal: 'left',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'left',
        }}
      >
        <NewTrigger handleClose={handleClose} />
      </Popover>
      <TriggerDialog
        triggerId={dialogTriggerId}
        open={openDialog}
        handleClose={handleCloseDialog}
        handleDelete={handleDelete}
        handleDeleteResource={handleDeleteResource}
      />
      <Button
        variant='contained'
        style={{ background: '#7A14E5', marginTop: '1em' }}
        onClick={handleNewTrigger}
      >
        <AddIcon /> New Trigger
      </Button>
    </>
  );
}
