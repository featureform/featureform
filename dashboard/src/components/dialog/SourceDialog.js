import Button from '@mui/material/Button';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import * as React from 'react';
import SourceDialogTable from './SourceDialogTable';

export default function SourceDialog({
  api,
  sourceName = '',
  sourceVariant = 'default',
}) {
  const [open, setOpen] = React.useState(false);
  const [columns, setColumns] = React.useState([]);
  const [rowList, setRowList] = React.useState([]);

  React.useEffect(async () => {
    if (sourceName !== '' && open) {
      let response = await api.fetchSourceModalData(sourceName, sourceVariant);
      setColumns(response.columns);
      setRowList(response.rows);
    }
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
        Open Table Source
      </Button>
      <Dialog
        fullWidth={true}
        maxWidth={columns.length > 3 ? 'lg' : 'sm'}
        open={open}
        onClose={handleClose}
        aria-labelledby='dialog-title'
        aria-describedby='dialog-description'
      >
        <DialogTitle id='dialog-title' data-testid={'sourceTableTitleId'}>
          {sourceName.toUpperCase()}
        </DialogTitle>
        <DialogContent>
          <SourceDialogTable api={api} columns={columns} rowList={rowList} />
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
