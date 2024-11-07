// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import CloseIcon from '@mui/icons-material/Close';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import { Alert, Slide, Snackbar, Tooltip, Typography } from '@mui/material';
import Button from '@mui/material/Button';
import Dialog from '@mui/material/Dialog';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import IconButton from '@mui/material/IconButton';
import React, { useState } from 'react';

export const ERROR_MSG_MAX = 200;

export default function ErrorModal({ errorTxt = '', buttonTxt = '' }) {
  const [open, setOpen] = useState(false);
  const [isShowMore] = useState(errorTxt.length > ERROR_MSG_MAX);
  const [truncated] = useState(errorTxt.substring(0, ERROR_MSG_MAX));
  const [openSnack, setOpenSnack] = useState(false);

  const closeSnackBar = (_, reason) => {
    if (reason === 'clickaway') {
      return;
    }
    setOpenSnack(false);
  };

  const copyToClipBoard = (error = '') => {
    if (error) {
      navigator.clipboard.writeText(error);
      setOpenSnack(true);
    }
  };

  function transition(props) {
    return <Slide {...props} direction='right' />;
  }

  const handleClickOpen = () => {
    setOpen(true);
  };
  const handleClose = () => {
    setOpen(false);
  };

  return (
    <div>
      <Snackbar
        open={openSnack}
        autoHideDuration={1250}
        onClose={closeSnackBar}
        TransitionComponent={transition}
      >
        <Alert severity='success' onClose={closeSnackBar}>
          <Typography>Copied to clipboard!</Typography>
        </Alert>
      </Snackbar>
      <Typography
        style={{ whiteSpace: 'pre-line' }}
        data-testid='errorMessageId'
        variant='body1'
      >
        {`${truncated + (isShowMore ? '...' : '')}`}
      </Typography>
      {isShowMore && (
        <Button
          data-testid='openErrorModalId'
          variant='text'
          onClick={handleClickOpen}
          sx={{ paddingLeft: 0, fontSize: 15 }}
        >
          {buttonTxt}
        </Button>
      )}

      <Dialog
        fullWidth={true}
        open={open}
        onClose={handleClose}
        aria-labelledby='dialog-title'
        aria-describedby='dialog-description'
      >
        <DialogTitle id='dialog-title' data-testid={'errorModalTitleId'}>
          Error Message
          <Tooltip title='Copy to Clipboard'>
            <Button
              role='none'
              onClick={() => copyToClipBoard(errorTxt)}
              fontSize={11.5}
            >
              <ContentCopyIcon />
            </Button>
          </Tooltip>
          <IconButton
            data-testid={'errorModalCloseId'}
            sx={{ float: 'right' }}
            onClick={handleClose}
          >
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent>
          <Typography
            variant='body1'
            data-testid='fullTextContent'
            style={{ whiteSpace: 'pre-line', color: 'red' }}
          >
            {errorTxt}
          </Typography>
        </DialogContent>
      </Dialog>
    </div>
  );
}
