import {
  Alert,
  Paper,
  Snackbar,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material';
import * as React from 'react';

export default function SourceDialogTable({ columns = [], rowList = [] }) {
  const [open, setOpen] = React.useState(false);

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

  return (
    <div>
      <Snackbar open={open} autoHideDuration={1500} onClose={closeSnackBar}>
        <Alert severity='success' onClose={closeSnackBar}>
          Copied to clipboard!
        </Alert>
      </Snackbar>
      <TableContainer component={Paper}>
        <Table sx={{ minWidth: 300 }} aria-label='Source Data Table'>
          <TableHead>
            <TableRow>
              {columns.map((col, i) => (
                <TableCell
                  key={col + i}
                  data-testid={col + i}
                  align={i === 0 ? 'left' : 'right'}
                >
                  {col}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {rowList.map((currentRow, index) => (
              <TableRow
                key={'mainRow' + index}
                data-testid={'mainRow' + index}
                sx={{ '&:last-child td, &:last-child th': { border: 0 } }}
              >
                {currentRow.map((row, index) => (
                  <TableCell
                    key={row + index}
                    align={index === 0 ? 'left' : 'right'}
                  >
                    <Typography
                      onClick={copyToClipBoard}
                      fontSize={11}
                      style={{ cursor: 'pointer' }}
                    >
                      {row}
                    </Typography>
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </div>
  );
}
