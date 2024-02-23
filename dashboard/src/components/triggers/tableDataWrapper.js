import SearchIcon from '@mui/icons-material/Search';
import { Box, IconButton, InputAdornment, TextField } from '@mui/material';
import React, { useEffect, useState } from 'react';
import { useDataAPI } from '../../hooks/dataAPI';
import { useStyles } from './styles';
import TriggerDataGrid from './triggerDataGrid';

export default function TableDataWrapper() {
  const classes = useStyles();
  const dataAPI = useDataAPI();
  const ENTER_KEY = 'Enter';
  const [searchQuery, setSearchQuery] = useState('');
  const [triggerList, setTriggerList] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(async () => {
    // let data = await dataAPI.getTriggers(searchQuery);
    if (loading) {
      let data = [
        {
          id: 1,
          name: 'Monday at Noon',
          type: 'Schedule Trigger',
          schedule: '0 12**MON',
          detail: 'Pending Details',
        },
      ];
      setTriggerList(data);
    }
    setLoading(false);
  }, [loading]);

  const handleSearch = (searchArg = '') => {
    setSearchQuery(searchArg);
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
          <TextField
            placeholder='Search'
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
                // todox: odd case since i'm tracking 2 search props.
                // the one in the searchparams, and also the input's itself.
                // the searchParams, won't update unless you hit ENTER.
                // so you can ultimately search with a stale searchParam.searchText value
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
              'aria-label': 'search',
              'data-testid': 'searcInputId',
            }}
          />
        </Box>
      </Box>
      <TriggerDataGrid triggerList={triggerList} />
    </>
  );
}
