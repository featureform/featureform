import CloseIcon from '@mui/icons-material/Close';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import { Alert, Slide, Snackbar, Tooltip, Typography } from '@mui/material';
import Button from '@mui/material/Button';
import Dialog from '@mui/material/Dialog';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import IconButton from '@mui/material/IconButton';
import * as React from 'react';

export default function ErrorModal({ errorTxt = '', buttonTxt = '' }) {
  const MAX = 200;
  const [open, setOpen] = React.useState(false);
  const [isShowMore] = React.useState(errorTxt.length > MAX);
  const [truncated] = React.useState(errorTxt.substring(0, MAX));
  const [openSnack, setOpenSnack] = React.useState(false);

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
      <Typography variant='body1'>
        {truncated}
        {isShowMore && (
          <Button
            data-testid='openErrorModalId'
            variant='text'
            onClick={handleClickOpen}
            sx={{ paddingLeft: 0, fontSize: 15 }}
          >
            {`...${buttonTxt}`}
          </Button>
        )}
      </Typography>

      <Dialog
        fullWidth={true}
        maxWidth={'xl'}
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
          <Typography variant='body1'>{errorTxt}</Typography>
        </DialogContent>
      </Dialog>
    </div>
  );
}
