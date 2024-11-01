// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import RefreshIcon from '@mui/icons-material/Refresh';
import SearchIcon from '@mui/icons-material/Search';
import {
  Box,
  Button,
  Chip,
  CircularProgress,
  FormControl,
  IconButton,
  InputAdornment,
  InputLabel,
  MenuItem,
  Select,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import { styled } from '@mui/system';
import { useRouter } from 'next/router';
import React, { useEffect, useState } from 'react';
import { useDataAPI } from '../../hooks/dataAPI';
import { WHITE } from '../../styles/theme';
import TaskRunDataGrid from './taskRunDataGrid';

const PREFIX = 'TableDataWrapper';

const classes = {
  inputRow: `${PREFIX}-inputRow`,
  activeButton: `${PREFIX}-activeButton`,
  activeChip: `${PREFIX}-activeChip`,
  inactiveChip: `${PREFIX}-inactiveChip`,
  inactiveButton: `${PREFIX}-inactiveButton`,
  buttonText: `${PREFIX}-buttonText`,
  filterInput: `${PREFIX}-filterInput`,
  taskCardBox: `${PREFIX}-taskCardBox`,
};

const Root = styled('div')(() => ({
  [`& .${classes.inputRow}`]: {
    paddingBottom: 10,
    '& button': {
      height: 40,
    },
  },
  [`& .${classes.activeButton}`]: {
    color: '#000000',
    fontWeight: 'bold',
  },
  [`& .${classes.activeChip}`]: {
    color: WHITE,
    background: '#7A14E5',
    height: 20,
    width: 25,
    fontSize: 11,
  },
  [`& .${classes.inactiveChip}`]: {
    color: WHITE,
    background: '#BFBFBF',
    height: 20,
    width: 25,
    fontSize: 11,
  },
  [`& .${classes.inactiveButton}`]: {
    color: '#000000',
    background: WHITE,
  },
  [`& .${classes.buttonText}`]: {
    textTransform: 'none',
    paddingRight: 10,
  },
  [`& .${classes.filterInput}`]: {
    minWidth: 225,
    height: 40,
  },
  [`& .${classes.taskCardBox}`]: {
    height: 750,
    width: 700,
  },
}));

export default function TableDataWrapper() {
  const router = useRouter();
  const { name: queryName } = router.query;
  const dataAPI = useDataAPI();
  const FILTER_STATUS_ALL = 'ALL';
  const FILTER_STATUS_ACTIVE = 'ACTIVE';
  const FILTER_STATUS_COMPLETE = 'COMPLETE';
  const SORT_STATUS = 'STATUS';
  const SORT_DATE = 'DATE';
  const SORT_RUN_ID = 'RUN_ID';
  const ENTER_KEY = 'Enter';

  const DEFAULT_PARAMS = Object.freeze({
    status: FILTER_STATUS_ALL,
    sortBy: SORT_DATE,
    searchText: '',
    pageSize: 15,
    offset: 0,
  });

  const initialSearchTxt = queryName ?? '';
  const [searchParams, setSearchParams] = useState({
    ...DEFAULT_PARAMS,
    searchText: initialSearchTxt,
  });

  const [searchQuery, setSearchQuery] = useState(initialSearchTxt);
  const [totalCount, setTotalCount] = useState(0);
  const [taskRunList, setTaskRunList] = useState([]);
  const [loading, setLoading] = useState(true);
  const [allCount] = useState();
  const [activeCount] = useState();
  const [completeCount] = useState();

  useEffect(() => {
    const getTaskRuns = async () => {
      if (loading) {
        let data = await dataAPI.getTaskRuns(searchParams);
        setTaskRunList(data?.list ?? []);
        setTotalCount(data?.count ?? 0);
        const timeout = setTimeout(() => {
          setLoading(false);
        }, 750);
        return () => {
          if (timeout) {
            clearTimeout(timeout);
          }
        };
      }
    };
    getTaskRuns();
  }, [searchParams, loading]);

  const handleStatusBtnSelect = (statusType = FILTER_STATUS_ALL) => {
    setSearchParams({ ...searchParams, status: statusType });
    setLoading(true);
  };

  const handleSortBy = (event) => {
    let value = event?.target?.value ?? '';
    setSearchParams({ ...searchParams, sortBy: value });
    setLoading(true);
  };

  const handleSearch = (searchArg = '') => {
    setSearchParams({ ...searchParams, searchText: searchArg });
    setLoading(true);
  };

  const handleReloadRequest = () => {
    if (!loading) {
      setLoading(true);
    }
  };

  const handlePageChange = (page) => {
    setSearchParams({ ...searchParams, offset: page });
    setLoading(true);
  };

  const clearInputs = () => {
    setSearchParams({ ...DEFAULT_PARAMS });
    setSearchQuery('');
    setLoading(true);
  };

  return (
    <Root>
      <Box className={classes.inputRow}>
        <Button
          variant='text'
          className={
            searchParams.status === FILTER_STATUS_ALL
              ? classes.activeButton
              : classes.inactiveButton
          }
          onClick={() => handleStatusBtnSelect(FILTER_STATUS_ALL)}
        >
          <Typography variant='button' className={classes.buttonText}>
            All
          </Typography>
          <Chip
            label={allCount}
            data-testid='allId'
            className={
              searchParams.status === FILTER_STATUS_ALL
                ? classes.activeChip
                : classes.inactiveChip
            }
          />
        </Button>
        <Button
          variant='text'
          className={
            searchParams.status === FILTER_STATUS_ACTIVE
              ? classes.activeButton
              : classes.inactiveButton
          }
          onClick={() => handleStatusBtnSelect(FILTER_STATUS_ACTIVE)}
        >
          <Typography variant='button' className={classes.buttonText}>
            Active
          </Typography>
          <Chip
            label={activeCount}
            data-testid='activeId'
            className={
              searchParams.status === FILTER_STATUS_ACTIVE
                ? classes.activeChip
                : classes.inactiveChip
            }
          />
        </Button>
        <Button
          variant='text'
          className={
            searchParams.status === FILTER_STATUS_COMPLETE
              ? classes.activeButton
              : classes.inactiveButton
          }
          onClick={() => handleStatusBtnSelect(FILTER_STATUS_COMPLETE)}
        >
          <Typography variant='button' className={classes.buttonText}>
            Complete
          </Typography>
          <Chip
            label={completeCount}
            data-testid='completeId'
            className={
              searchParams.status === FILTER_STATUS_COMPLETE
                ? classes.activeChip
                : classes.inactiveChip
            }
          />
        </Button>

        <Box style={{ float: 'right' }}>
          <FormControl style={{ paddingRight: '15px' }}>
            <InputLabel shrink={true} id='sortId'>
              Sort By
            </InputLabel>
            <Select
              value={searchParams.sortBy}
              onChange={handleSortBy}
              label='Sort By'
              notched
              className={classes.filterInput}
            >
              <MenuItem value={SORT_STATUS}>Status</MenuItem>
              <MenuItem value={SORT_DATE}>Date</MenuItem>
              <MenuItem value={SORT_RUN_ID}>RunId</MenuItem>
            </Select>
          </FormControl>
          <FormControl>
            <TextField
              size='small'
              InputLabelProps={{ shrink: true }}
              label='Search'
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
              inputProps={{
                'aria-label': 'search',
                'data-testid': 'searcInputId',
              }}
            />
          </FormControl>
          <Tooltip title='Refresh table' placement='top'>
            <IconButton size='large' onClick={handleReloadRequest}>
              {loading ? (
                <CircularProgress
                  size={'.85em'}
                  data-testid='circularProgressId'
                />
              ) : (
                <RefreshIcon data-testid='refreshIcon' />
              )}
            </IconButton>
          </Tooltip>
          <Tooltip title='Clear filter inputs' placement='top'>
            <IconButton size='large' onClick={clearInputs}>
              <img
                alt={'CLEAR'}
                data-testid='clearIcon'
                src={'/static/clearIcon.svg'}
              />
            </IconButton>
          </Tooltip>
        </Box>
      </Box>
      <TaskRunDataGrid
        count={totalCount}
        currentPage={searchParams.offset}
        setPage={handlePageChange}
        taskRunList={taskRunList?.map(function (object) {
          return {
            ...object,
            id: object.taskRun.taskId + '.' + object.taskRun.runId,
          };
        })}
      />
    </Root>
  );
}
