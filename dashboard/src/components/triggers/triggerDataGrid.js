import RemoveCircleOutlineIcon from '@mui/icons-material/RemoveCircleOutline';
import { IconButton } from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import React, { useState } from 'react';
import { getPageSizeProp } from '../../components/tasks/taskRunDataGrid';
import { useDataAPI } from '../../hooks/dataAPI';
import TriggerDialog from './triggerDialog';

export default function TriggerDataGrid({ triggerList = [], refresh }) {
  const [openDialog, setOpenDialog] = useState(false);
  const [dialogTriggerId, setDialogTriggerId] = useState();
  const [rowDelete, setRowDelete] = useState(false);
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
          <IconButton
            data-testid={`triggerDelete-${params?.id}`}
            onClick={() => {
              setRowDelete(true);
              handleRowSelect(params);
            }}
          >
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

  const handleRowSelect = (selectedRow) => {
    setOpenDialog(true);
    setDialogTriggerId(selectedRow?.row?.id);
  };

  const handleCloseDialog = () => {
    setRowDelete(false);
    setOpenDialog(false);
  };

  const mainPageSize = getPageSizeProp(triggerList?.length);

  return (
    <>
      <DataGrid
        disableVirtualization
        onRowClick={handleRowSelect}
        autoHeight
        density='compact'
        sx={{
          minWidth: 300,
          '& .MuiDataGrid-cell:focus': {
            outline: 'none',
          },
          '& .MuiDataGrid-row:hover': {
            cursor: 'pointer',
          },
        }}
        aria-label='Triggers'
        rows={triggerList ?? []}
        rowsPerPageOptions={[5, 10, 15]}
        columns={columns}
        initialState={{
          pagination: { paginationModel: { page: 1, pageSize: mainPageSize } },
        }}
        pageSize={mainPageSize}
      />
      <TriggerDialog
        triggerId={dialogTriggerId}
        open={openDialog}
        handleClose={handleCloseDialog}
        handleDelete={handleDelete}
        handleDeleteResource={handleDeleteResource}
        rowDelete={rowDelete}
      />
    </>
  );
}
