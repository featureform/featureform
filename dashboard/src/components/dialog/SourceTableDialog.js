import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from '@mui/material';
import Button from '@mui/material/Button';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import * as React from 'react';

//todo: this should be a dummy component. the parent should make the endpoint request.
export default function SourceTableDialog({
  tableName = '',
  columns = [],
  rows = [],
}) {
  const [open, setOpen] = React.useState(false);

  const handleClickOpen = () => {
    setOpen(true);
  };

  const handleClose = () => {
    setOpen(false);
  };

  //todo: remove this
  if (columns.length === 0) {
    columns.push('Person Type', 'Name', 'Language', 'Last Updated');
  }

  function createData(type, name, language, lastUpdated) {
    return { type, name, language, lastUpdated };
  }

  if (rows.length === 0) {
    for (let i = 0; i < 15; i++) {
      rows.push(
        createData('Employee', 'Anthony', 'English', new Date().toDateString()),
        createData('Contractor', 'Claire', 'German', new Date().toDateString()),
        createData('External', 'Susie', 'Italian', new Date().toDateString())
      );
    }
  }

  //todo: end remove

  return (
    <div>
      <Button
        data-testid='sourceTableOpenId'
        variant='outlined'
        onClick={handleClickOpen}
      >
        Open Table Source
      </Button>
      <Dialog
        fullWidth={true}
        maxWidth={'md'}
        open={open}
        onClose={handleClose}
        aria-labelledby='dialog-title'
        aria-describedby='dialog-description'
      >
        <DialogTitle id='dialog-title' data-testid={'sourceTableTitleId'}>
          {tableName}
        </DialogTitle>
        <DialogContent>
          <TableContainer component={Paper}>
            <Table sx={{ minWidth: 500 }} aria-label='Source Data Table'>
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
                {rows.map((row, index) => (
                  <TableRow
                    key={row.name + index}
                    data-testid={row.name + index}
                    sx={{ '&:last-child td, &:last-child th': { border: 0 } }}
                  >
                    <TableCell component='th' scope='row'>
                      {row.type}
                    </TableCell>
                    <TableCell align='right'>{row.name}</TableCell>
                    <TableCell align='right'>{row.language}</TableCell>
                    <TableCell align='right'>{row.lastUpdated}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </DialogContent>
        <DialogActions>
          <Button data-testid={'sourceTableCloseId'} onClick={handleClose}>
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </div>
  );
}
