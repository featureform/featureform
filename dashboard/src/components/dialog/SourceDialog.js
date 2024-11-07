// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import CloseIcon from '@mui/icons-material/Close';
import { Box, CircularProgress } from '@mui/material';
import Button from '@mui/material/Button';
import Dialog from '@mui/material/Dialog';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import IconButton from '@mui/material/IconButton';
import React, { useEffect, useState } from 'react';
import SourceDialogTable from './SourceDialogTable';

export default function SourceDialog({
  api,
  btnTxt = 'Preview Result',
  sourceName = '',
  sourceVariant = '',
}) {
  const [open, setOpen] = useState(false);
  const [columns, setColumns] = useState([]);
  const [rowList, setRowList] = useState([]);
  const [error, setError] = useState('');
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const getData = async () => {
      if (sourceName && sourceVariant && open && api) {
        setIsLoading(true);
        let response = await api.fetchSourceModalData(
          sourceName,
          sourceVariant
        );
        if (response?.columns) {
          setColumns(response.columns ?? []);
          setRowList(response.rows ?? []);
        } else {
          setError(response);
        }
        setIsLoading(false);
      }
    };
    getData();
  }, [sourceName, sourceVariant, open]);

  const handleClickOpen = () => {
    setOpen(true);
  };
  const handleClose = () => {
    setOpen(false);
  };

  return (
    <div>
      <Button
        data-testid='sourceTableOpenId'
        variant='outlined'
        onClick={handleClickOpen}
      >
        {btnTxt}
      </Button>
      <Dialog
        fullWidth={true}
        maxWidth={columns.length > 3 ? 'xl' : 'sm'}
        open={open}
        onClose={handleClose}
        aria-labelledby='dialog-title'
        aria-describedby='dialog-description'
      >
        <DialogTitle id='dialog-title' data-testid={'sourceTableTitleId'}>
          {`${sourceName} - ${sourceVariant}`}
          <IconButton
            data-testid={'sourceTableCloseId'}
            sx={{ float: 'right' }}
            onClick={handleClose}
          >
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent>
          {isLoading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center' }}>
              <CircularProgress />
            </Box>
          ) : error === '' ? (
            <SourceDialogTable columns={columns} rowList={rowList} />
          ) : (
            <div data-testid='errorMessageId'>{error}</div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}
