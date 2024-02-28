import AddIcon from '@mui/icons-material/Add';
import SearchIcon from '@mui/icons-material/Search';
import {
  Box,
  Button,
  IconButton,
  InputAdornment,
  Popover,
  TextField,
} from '@mui/material';
import React, { useEffect, useState } from 'react';
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

  useEffect(async () => {
    if (loading) {
      let data = await dataAPI.getTriggers(searchQuery);
      if (Array.isArray(data)) {
        setTriggerList(data);
      } else {
        setTriggerList([]);
      }
    }
    setLoading(false);
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
      <Box
        className={classes.inputRow}
        display={'flex'}
        justifyContent={'flex-end'}
      >
        <Box>
          <Button
            variant='contained'
            style={{ background: '#7A14E5', marginTop: '1em' }}
            onClick={handleNewTrigger}
          >
            <AddIcon /> New Trigger
          </Button>
          <TextField
            placeholder='Search Triggers'
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
      </Box>
      <TriggerDataGrid triggerList={triggerList} refresh={refresh} />
      <Popover
        open={openNew}
        anchorReference='anchorPosition'
        anchorPosition={{ top: 500, left: 275 }}
        onClose={handleClose}
        anchorOrigin={{
          vertical: 'top',
          horizontal: 'left',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'left',
        }}
      >
        <NewTrigger handleClose={handleClose} />
      </Popover>
    </>
  );
}
