import {
  Alert,
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
import * as React from 'react';
import Barchart from '../../components/charts/Barchart';
import UniqueValues from '../../components/charts/UniqueValues';

export default function SourceDialogTable({
  stats = [],
  columns = [],
  rowList = [],
}) {
  const textEllipsis = {
    whiteSpace: 'nowrap',
    maxWidth: columns?.length > 1 ? '230px' : '500px',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    cursor: 'pointer',
  };

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
                <TableCell
                  key={col + i}
                  data-testid={col + i}
                  align={i === 0 ? 'left' : 'right'}
                >
                  {`${col}`}
                </TableCell>
              ))}
            </TableRow>
            {stats?.length ? (
              <TableRow>
                {stats?.map((statObj, index) => (
                  <TableCell key={index} align={'right'}>
                    {['numeric', 'boolean'].includes(statObj.type) ? (
                      <Barchart
                        categories={statObj.categories}
                        categoryCounts={statObj.categoryCounts}
                        type={statObj.type}
                      />
                    ) : (
                      <UniqueValues count={statObj?.categoryCounts[0]} />
                    )}
                  </TableCell>
                ))}
              </TableRow>
            ) : null}
          </TableHead>
          <TableBody>
            {rowList?.map((currentRow, index) => (
              <TableRow
                key={'mainRow' + index}
                data-testid={'mainRow' + index}
                sx={{ '&:last-child td, &:last-child th': { border: 0 } }}
              >
                {currentRow?.map((row, index) => (
                  <TableCell
                    key={row + index}
                    align={index === 0 ? 'left' : 'right'}
                    sx={{ maxHeight: '50px' }}
                  >
                    <Tooltip title='Copy to Clipboard'>
                      <Typography
                        onClick={copyToClipBoard}
                        fontSize={11}
                        style={textEllipsis}
                      >
                        {`${row}`}
                      </Typography>
                    </Tooltip>
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </>
  );
}
