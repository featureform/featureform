// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { TextField } from '@mui/material';
import { styled } from '@mui/system';
import { useRouter } from 'next/router';
import React, { useEffect, useState } from 'react';
import { FEATUREFORM_DARK_GRAY, WHITE } from '../../styles/theme';

const ENTER_KEY = 'Enter';
const DEFAULT_FILL = FEATUREFORM_DARK_GRAY;

const CustomTextField = styled(TextField)(() => ({
  background: WHITE,
  width: 350,
  '& label': {
    color: DEFAULT_FILL,
  },
  '& label.Mui-focused': {
    color: DEFAULT_FILL,
  },
  '& .MuiOutlinedInput-root': {
    color: DEFAULT_FILL,
    borderColor: DEFAULT_FILL,
    '& fieldset': {
      borderColor: DEFAULT_FILL,
    },
    '&.Mui-focused fieldset': {
      borderColor: DEFAULT_FILL,
    },
  },
}));

const SearchBar = () => {
  const router = useRouter();
  const [searchText, setSearchText] = useState('');

  function handleSearch(event) {
    event.preventDefault();
    let uri = '/query?q=' + searchText?.trim();
    router.push(uri);
  }

  useEffect(() => {
    if (router.query) {
      if ('q' in router.query) {
        setSearchText(router.query.q);
      } else {
        setSearchText('');
      }
    }
  }, [router.query]);

  return (
    <CustomTextField
      size='small'
      label='Search'
      InputLabelProps={{ shrink: true }}
      onChange={(event) => {
        const rawText = event.target.value;
        if (rawText === '') {
          // user is deleting the text field. allow this and clear out state
          setSearchText(rawText);
          return;
        }
        const searchText = event.target.value ?? '';
        if (searchText.trim()) {
          setSearchText(searchText);
        }
      }}
      value={searchText}
      onKeyDown={(event) => {
        if (event.key === ENTER_KEY && searchText) {
          handleSearch(event);
        }
      }}
      inputProps={{
        'aria-label': 'search',
        'data-testid': 'searchInputId',
      }}
    />
  );
};

export default SearchBar;
