// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import {
  Alert,
  Box,
  Paper,
  Slide,
  Snackbar,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Typography,
} from '@mui/material';
import React, { useState } from 'react';

export default function SourceDialogTable({ columns = [], rowList = [] }) {
  const textEllipsis = {
    whiteSpace: 'nowrap',
    maxWidth: columns?.length > 1 ? '100%' : '500px',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    cursor: 'pointer',
  };

  const noRecords = {
    textAlign: 'center',
    marginTop: 10,
  };

  const [open, setOpen] = useState(false);
  const closeSnackBar = (_, reason) => {
    if (reason === 'clickaway') {
      return;
    }
    setOpen(false);
  };

  const copyToClipBoard = (event) => {
    if (event?.target) {
      navigator.clipboard.writeText(event.target.textContent);
      setOpen(true);
    }
  };

  function transition(props) {
    return <Slide {...props} direction='right' />;
  }

  return (
    <>
      <Snackbar
        open={open}
        autoHideDuration={1250}
        onClose={closeSnackBar}
        TransitionComponent={transition}
      >
        <Alert severity='success' onClose={closeSnackBar}>
          <Typography>Copied to clipboard!</Typography>
        </Alert>
      </Snackbar>
      <TableContainer component={Paper}>
        <Table sx={{ minWidth: 300 }} aria-label='Source Data Table'>
          <TableHead>
            <TableRow>
              {columns?.map((col, i) => (
                <React.Fragment key={i}>
                  <TableCell
                    key={col + i}
                    data-testid={col + i}
                    align={i === 0 ? 'left' : 'right'}
                  >
                    {`${col}`}
                  </TableCell>
                </React.Fragment>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {rowList?.map((currentRow, index) => (
              <TableRow
                key={'mainRow' + index}
                data-testid={'mainRow' + index}
                sx={{ '&:last-child td, &:last-child th': { border: 0 } }}
              >
                {currentRow?.map((row, index) => (
                  <React.Fragment key={index}>
                    <TableCell
                      key={row + index}
                      align={index === 0 ? 'left' : 'right'}
                      sx={{ maxHeight: '50px' }}
                    >
                      <Tooltip title='Copy to Clipboard'>
                        <Typography
                          onClick={copyToClipBoard}
                          fontSize={11.5}
                          style={textEllipsis}
                        >
                          {`${row}`}
                        </Typography>
                      </Tooltip>
                    </TableCell>
                  </React.Fragment>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      {rowList?.length ? null : (
        <Box>
          <Typography style={noRecords} fontWeight={'bold'}>
            No records to display
          </Typography>
        </Box>
      )}
    </>
  );
}
