import CloseIcon from '@mui/icons-material/Close';
import { Box, CircularProgress, Divider } from '@mui/material';
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
}) {
  const [error] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [details, setDetails] = useState({});
  const dataAPI = useDataAPI();

  useEffect(async () => {
    if (open && triggerId) {
      setIsLoading(true);
      const data = await dataAPI.getTriggerDetails(triggerId);
      if (typeof data == 'object' && data.owner) {
        setDetails(data);
      }
      setIsLoading(false);
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
          {isLoading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center' }}>
              <CircularProgress />
            </Box>
          ) : error === '' ? (
            <TriggerDetail
              handleClose={handleClose}
              handleDelete={handleDelete}
              handleDeleteResource={handleDeleteResource}
              details={details}
            />
          ) : (
            <div data-testid='errorMessageId'>{error}</div>
          )}
        </DialogContent>
      </Dialog>
    </Box>
  );
}
