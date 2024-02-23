import CloseIcon from '@mui/icons-material/Close';
import { Box, CircularProgress, Divider } from '@mui/material';
import Dialog from '@mui/material/Dialog';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import IconButton from '@mui/material/IconButton';
import React, { useEffect, useState } from 'react';
import TriggerDetail from './triggerDetail';

export default function TriggerDialog({ open, handleClose }) {
  const [error] = useState('');
  const [isLoading] = useState(false);

  useEffect(async () => {
    if (open) {
      console.log('fetch server data');
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
          <strong>{`Trigger Detail -${' 3pm daily'}`}</strong>
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
            <TriggerDetail handleClose={handleClose} />
          ) : (
            <div data-testid='errorMessageId'>{error}</div>
          )}
        </DialogContent>
      </Dialog>
    </Box>
  );
}
