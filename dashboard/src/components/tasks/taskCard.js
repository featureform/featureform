import CloseFullscreenIcon from '@mui/icons-material/CloseFullscreen';
import DoubleArrowIcon from '@mui/icons-material/DoubleArrow';
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown';
import KeyboardArrowUpIcon from '@mui/icons-material/KeyboardArrowUp';
import MoreHorizIcon from '@mui/icons-material/MoreHoriz';
import RefreshIcon from '@mui/icons-material/Refresh';
import {
  Grid,
  IconButton,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from '@mui/material';
import React from 'react';

export default function TaskCard({ taskRecord }) {
  return (
    <div
      style={{
        height: 700,
        width: 600,
      }}
    >
      <div>
        <span style={{ float: 'left' }}>
          <IconButton variant='' size='small'>
            <DoubleArrowIcon />
          </IconButton>
          <IconButton variant='' size='small'>
            <CloseFullscreenIcon />
          </IconButton>
          <IconButton variant='' size='small'>
            <KeyboardArrowUpIcon />
          </IconButton>
          <IconButton variant='' size='small'>
            <KeyboardArrowDownIcon />
          </IconButton>
        </span>
        <span style={{ float: 'right' }}>
          <IconButton variant='' size='small'>
            <RefreshIcon />
          </IconButton>
          <IconButton variant='' size='small'>
            <MoreHorizIcon />
          </IconButton>
        </span>
      </div>
      <Grid style={{ padding: 12 }} container>
        <Grid item xs={6} justifyContent='flex-start'>
          <Typography variant='h5'>{taskRecord.name}</Typography>
        </Grid>
        <Grid item xs={6} justifyContent='center'>
          <Typography variant='h5'>Status: {taskRecord.status}</Typography>
        </Grid>
        {/*  */}
        <Grid
          item
          xs={6}
          justifyContent='flex-start'
          style={{ paddingTop: 50 }}
        >
          <Typography variant='h5'>Logs/Errors</Typography>
        </Grid>
        <Grid item xs={6} justifyContent='center' style={{ paddingTop: 50 }}>
          <Typography variant='h5'>Task Run Details</Typography>
        </Grid>
        {/*  */}
        <Grid item xs={6} justifyContent='flex-start'>
          <Typography>
            <TextField
              style={{ width: '90%' }}
              variant='filled'
              disabled
              value='Some form of error, etc.'
              multiline
              minRows={3}
            ></TextField>
          </Typography>
        </Grid>
        <Grid item xs={6} justifyContent='center'>
          <Typography>
            <TextField
              style={{ width: '90%' }}
              variant='filled'
              disabled
              value='Run Detail field with some extra data'
              multiline
              minRows={3}
            ></TextField>
          </Typography>
        </Grid>
        <Grid
          item
          xs={12}
          justifyContent='flex-start'
          style={{ paddingTop: 50 }}
        >
          <Typography variant='h5'>Previous Task Runs</Typography>
        </Grid>
        <Grid item xs={12} justifyContent='flex-start'></Grid>
        <Grid item xs={12} justifyContent='center'>
          <TableContainer component={Paper}>
            <Table sx={{ maxWidth: 500 }} aria-label='simple table'>
              <TableHead>
                <TableRow>
                  <TableCell>Date/Time</TableCell>
                  <TableCell align='right'>Status</TableCell>
                  <TableCell align='right'>Link to Run Task</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                <TableRow>
                  <TableCell component='th' scope='row'>
                    2024-06-15T
                  </TableCell>
                  <TableCell align='right'>Pending</TableCell>
                  <TableCell align='right'>
                    <a href='#'>future-link</a>
                  </TableCell>
                </TableRow>
              </TableBody>
            </Table>
          </TableContainer>
        </Grid>
      </Grid>
    </div>
  );
}
