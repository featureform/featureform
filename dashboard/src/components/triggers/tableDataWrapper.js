import SearchIcon from '@mui/icons-material/Search';
import {
  Box,
  Button,
  IconButton,
  InputAdornment,
  Popover,
  TextField,
  Typography,
} from '@mui/material';
import React, { useEffect, useRef, useState } from 'react';
import { useDataAPI } from '../../hooks/dataAPI';
import NewTrigger from './newTrigger';
import { useStyles } from './styles';
import TriggerDataGrid from './triggerDataGrid';

export default function TableDataWrapper() {
  const classes = useStyles();
  const dataAPI = useDataAPI();
  const ENTER_KEY = 'Enter';
  const [searchQuery, setSearchQuery] = useState('');
  const [triggerList, setTriggerList] = useState([]);
  const [openNew, setOpenNew] = useState(false);
  const [loading, setLoading] = useState(true);
  const ref = useRef();

  useEffect(async () => {
    if (loading) {
      let data = await dataAPI.getTriggers(searchQuery);
      if (Array.isArray(data)) {
        setTriggerList(data);
      } else {
        setTriggerList([]);
      }
      setLoading(false);
    }
  }, [loading]);

  const handleSearch = (searchArg = '') => {
    setSearchQuery(searchArg);
    setLoading(true);
  };

  const handleNewTrigger = () => {
    if (!openNew) {
      setOpenNew(true);
    }
  };

  function handleClose() {
    setOpenNew(false);
    refresh?.();
  }

  const refresh = () => {
    setLoading(true);
  };
  return (
    <>
      <Box className={classes.inputRow}>
        <Button
          variant='contained'
          style={{ background: '#7A14E5', marginRight: '1em' }}
          onClick={handleNewTrigger}
          data-testid='newTriggerId'
          ref={ref}
        >
          <>
            <Typography
              variant='button'
              textTransform={'none'}
              fontWeight={'bold'}
            >
              New Trigger
            </Typography>
          </>
        </Button>
        <TextField
          label='Search'
          InputLabelProps={{ shrink: true }}
          size='small'
          onChange={(event) => {
            const rawText = event.target.value;
            if (rawText === '') {
              // user is deleting the text field. allow this and clear out state
              setSearchQuery(rawText);
              handleSearch('');
              return;
            }
            const searchText = event.target.value ?? '';
            if (searchText.trim()) {
              setSearchQuery(searchText);
            }
          }}
          value={searchQuery}
          onKeyDown={(event) => {
            if (event.key === ENTER_KEY && searchQuery) {
              handleSearch(searchQuery);
            }
          }}
          InputProps={{
            endAdornment: (
              <InputAdornment position='end'>
                <IconButton>
                  <SearchIcon />
                </IconButton>
              </InputAdornment>
            ),
          }}
          className={classes.filterInput}
          inputProps={{
            'aria-label': 'search triggers',
            'data-testid': 'searcInputId',
          }}
        />
      </Box>
      <TriggerDataGrid triggerList={triggerList} refresh={refresh} />
      <Popover
        open={openNew}
        anchorReference='anchorEl'
        onClose={handleClose}
        anchorEl={ref?.current}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'left',
        }}
      >
        <NewTrigger handleClose={handleClose} />
      </Popover>
    </>
  );
}
