import CloseIcon from '@mui/icons-material/Close';
import { Box, CircularProgress } from '@mui/material';
import Button from '@mui/material/Button';
import Dialog from '@mui/material/Dialog';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import IconButton from '@mui/material/IconButton';
import * as React from 'react';
import SourceDialogTable from './SourceDialogTable';

export default function SourceDialog({
  api,
  btnTxt = 'Preview Result',
  type = 'Source',
  sourceName = '',
  sourceVariant = '',
}) {
  const [open, setOpen] = React.useState(false);
  const [columns, setColumns] = React.useState([]);
  const [rowList, setRowList] = React.useState([]);
  const [stats, setStats] = React.useState([]);
  const [error, setError] = React.useState('');
  const [isLoading, setIsLoading] = React.useState(true);
  const [skipIndexList, setSkipIndexList] = React.useState([]);

  React.useEffect(async () => {
    if (sourceName && sourceVariant && open && api) {
      setIsLoading(true);
      let response;

      if (type === 'Source') {
        response = await api.fetchSourceModalData(sourceName, sourceVariant);
      } else if (type === 'Feature') {
        response = await api.fetchFeatureFileStats(sourceName, sourceVariant);
      }
      if (response?.columns && response?.rows) {
        if (type === 'Feature') {
          let skipList = [];
          response.columns?.map((col, index) => {
            if (
              col === 'ts' &&
              response.stats?.[index]?.string_categories?.[0] === 'unique' &&
              response.stats?.[index]?.categoryCounts?.[0] === 1
            ) {
              skipList.push(index);
              return false;
            }
            return true;
          });
          setSkipIndexList(skipList);
        }
        setColumns(response.columns);
        setRowList(response.rows);
        setStats(response.stats);
      } else {
        setError(response);
      }
      setIsLoading(false);
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
          {`${sourceName.toUpperCase()} - ${sourceVariant}`}
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
            <SourceDialogTable
              stats={stats}
              columns={columns}
              rowList={rowList}
              skipIndexList={skipIndexList}
            />
          ) : (
            <div data-testid='errorMessageId'>{error}</div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}
