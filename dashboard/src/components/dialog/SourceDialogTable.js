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
  skipIndexList = [],
}) {
  const textEllipsis = {
    whiteSpace: 'nowrap',
    maxWidth: columns?.length > 1 ? '100%' : '500px',
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
                <React.Fragment key={i}>
                  {!skipIndexList.includes(i) && (
                    <TableCell
                      key={col + i}
                      data-testid={col + i}
                      align={i === 0 ? 'left' : 'right'}
                    >
                      {`${col}`}
                    </TableCell>
                  )}
                </React.Fragment>
              ))}
            </TableRow>
            {stats?.length ? (
              <React.Fragment>
                <TableRow>
                  {stats?.map((statObj, index) => (
                    <React.Fragment key={index}>
                      {!skipIndexList.includes(index) && (
                        <TableCell
                          key={index}
                          align={index === 0 ? 'left' : 'right'}
                        >
                          {['numeric', 'boolean'].includes(statObj.type) ? (
                            <Barchart
                              categories={
                                statObj.type === 'boolean'
                                  ? statObj.string_categories
                                  : statObj.numeric_categories
                              }
                              categoryCounts={statObj.categoryCounts}
                              type={statObj.type}
                            />
                          ) : (
                            <UniqueValues count={statObj?.categoryCounts[0]} />
                          )}
                        </TableCell>
                      )}
                    </React.Fragment>
                  ))}
                </TableRow>
              </React.Fragment>
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
                  <React.Fragment key={index}>
                    {!skipIndexList.includes(index) && (
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
                    )}
                  </React.Fragment>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </>
  );
}
