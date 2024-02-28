import CloseIcon from '@mui/icons-material/Close';
import { Box, Divider } from '@mui/material';
import Dialog from '@mui/material/Dialog';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import IconButton from '@mui/material/IconButton';
import React, { useEffect, useState } from 'react';
import { useDataAPI } from '../../hooks/dataAPI';
import TriggerDetail from './triggerDetail';

export default function TriggerDialog({
  open,
  triggerId,
  handleClose,
  handleDelete,
  handleDeleteResource,
  rowDelete = false,
}) {
  const [error] = useState('');
  const [details, setDetails] = useState({});
  const dataAPI = useDataAPI();

  const refresh = async function () {
    const data = await dataAPI.getTriggerDetails(triggerId);
    if (typeof data == 'object' && data.owner) {
      setDetails(data);
    }
  };

  useEffect(async () => {
    if (open && triggerId) {
      refresh();
    }
  }, [open]);

  return (
    <Box>
      <Dialog
        fullWidth={true}
        open={open}
        aria-labelledby='dialog-title'
        aria-describedby='dialog-description'
      >
        <DialogTitle id='dialog-title' data-testid={'triggerDialogId'}>
          <strong>{`Trigger Detail - ${details?.trigger?.name}`}</strong>
          <IconButton sx={{ float: 'right' }} onClick={handleClose}>
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <Divider />
        <DialogContent>
          {error === '' ? (
            <TriggerDetail
              handleClose={handleClose}
              handleDelete={handleDelete}
              handleDeleteResource={async (e, triggerId, resourceId) => {
                if (handleDeleteResource) {
                  await handleDeleteResource?.(e, triggerId, resourceId);
                  await refresh();
                }
              }}
              details={details}
              rowDelete={rowDelete}
            />
          ) : (
            <div data-testid='errorMessageId'>{error}</div>
          )}
        </DialogContent>
      </Dialog>
    </Box>
  );
}
