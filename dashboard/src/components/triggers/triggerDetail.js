import RemoveCircleOutlineIcon from '@mui/icons-material/RemoveCircleOutline';
import {
  Alert,
  Autocomplete,
  Box,
  Button,
  createFilterOptions,
  IconButton,
  TextField,
  Typography,
} from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import React, { useEffect, useState } from 'react';
import { useDataAPI } from '../../hooks/dataAPI';

export const PRE_DELETE = 'Delete Trigger';
export const CONFIRM_DELETE = 'Confirm, Delete!';

export const DELETE_WARNING =
  'To remove the Trigger, please delete all associated Resources first.';
export const DELETE_FINAL_WARNING =
  'You are about to delete this trigger. Are you sure you want to proceed?';
export const DELETE_INFO = 'To delete the trigger, remove all resources first.';

export default function TriggerDetail({
  details = {},
  handleClose,
  handleDelete,
  handleDeleteResource,
  handleAddResource,
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
      headerName: 'Variant',
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
      field: 'delete',
      headerName: 'Delete',
      flex: 0,
      width: 100,
      editable: false,
      sortable: false,
      filterable: false,
      renderCell: function (params) {
        return (
          <IconButton
            data-testid={`deleteResource-${params?.id}`}
            onClick={(e) =>
              handleDeleteResource?.(e, details?.trigger?.id, params?.id)
            }
          >
            <RemoveCircleOutlineIcon fontSize='medium' />
          </IconButton>
        );
      },
    },
  ];

  const [userConfirm, setUserConfirm] = useState(rowDelete);
  const [resourceList, setResourceList] = useState([]);
  const [isDisabled, setIsDisabled] = useState(true);
  const dataAPI = useDataAPI();
  const OPTIONS_LIMIT = 10;
  const filterOptions = createFilterOptions({ limit: OPTIONS_LIMIT });

  // fetch the list of resources
  useEffect(async () => {
    const results = await dataAPI.searchResources('');
    if (Array.isArray(results)) {
      const variantResults = results.filter((v) => v.Type.includes('_VARIANT'));
      const nameVariantList = variantResults?.map((q) => {
        let name = `${q.Name?.toLowerCase()}`;
        let variant = `${q.Variant?.toLowerCase()}`;
        return {
          label: `${name} - ${variant}`,
          name: name,
          variant: variant,
        };
      });
      setResourceList(nameVariantList);
    } else {
      console.warn('Search results did not populate ok. Result:', results);
    }
    let shouldDisable = !(
      details?.resources?.length === undefined ||
      details?.resources?.length === 0
    );
    setIsDisabled(shouldDisable);
  }, [details]);

  let alertBody = null;
  if (userConfirm && isDisabled) {
    alertBody = (
      <Alert data-testid='deleteWarning' severity='warning'>
        {DELETE_WARNING}
      </Alert>
    );
  } else if (userConfirm && !isDisabled) {
    alertBody = (
      <Alert data-testid='deleteFinal' severity='error'>
        {DELETE_FINAL_WARNING}
      </Alert>
    );
  } else {
    alertBody = (
      <Alert data-testid='deleteInfo' severity='info'>
        {DELETE_INFO}
      </Alert>
    );
  }

  const handleUserSelect = (e, value) => {
    handleAddResource?.(e, details?.trigger?.id, value?.name, value?.variant);
  };

  return (
    <>
      <Box sx={{ marginBottom: '2em' }}>
        <Typography data-testid='detailTypeId'>
          Trigger Type: {details?.trigger?.type}
        </Typography>
        <Typography data-testid='detailScheduleId'>
          Schedule: {details?.trigger?.schedule}
        </Typography>
        <Typography data-testid='detailOwnerId'>
          Owner: {details?.owner}
        </Typography>
      </Box>
      <Box sx={{ marginBottom: '2em' }}>
        <Autocomplete
          filterOptions={filterOptions}
          disablePortal
          clearOnEscape
          data-testid='addResourceId'
          clearOnBlur
          options={resourceList}
          fullWidth
          renderInput={(params) => (
            <TextField {...params} label='Add Resource' />
          )}
          onChange={handleUserSelect}
          isOptionEqualToValue={(option, value) => {
            if (
              option.name === value.name &&
              option.variant === value.variant
            ) {
              return true;
            }
          }}
        />
      </Box>
      <DataGrid
        disableVirtualization
        density='compact'
        autoHeight
        sx={{
          '& .MuiDataGrid-cell:focus': {
            outline: 'none',
          },
          '& .MuiDataGrid-columnHeaderTitle': {
            fontWeight: 'bold',
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
      <Box sx={{ marginTop: '1em' }}>{alertBody}</Box>
      <Box
        sx={{
          marginTop: '1em',
          bottom: '1em',
          right: '1em',
          position: 'absolute',
        }}
      >
        <Button
          variant='contained'
          onClick={handleClose}
          sx={{ margin: '0.5em', background: '#FFFFFF', color: '#000000' }}
        >
          Cancel
        </Button>
        <span>
          <Button
            variant='contained'
            data-testid='deleteTriggerBtnId'
            disabled={isDisabled}
            onClick={() => {
              if (!isDisabled) {
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
      </Box>
    </>
  );
}
