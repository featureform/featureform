// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import {
  Accordion,
  Divider,
  FormControlLabel,
  TextField,
  Typography,
} from '@mui/material';
import { Box, styled } from '@mui/system';
import React, { useState } from 'react';
import { FEATUREFORM_DARK_GRAY } from 'styles/theme';

const FILTER_PANEL_GREY = '#F9F9FA';

export const STATUS_OPTIONS = [
  { key: 'No Status', value: 'NO_STATUS' },
  { key: 'Created', value: 'CREATED' },
  { key: 'Pending', value: 'PENDING' },
  { key: 'Ready', value: 'READY' },
  { key: 'Failed', value: 'FAILED' },
  { key: 'Running', value: 'RUNNING' },
  { key: 'Cancelled', value: 'CANCELLED' },
];

export const MODE_OPTIONS = [
  { key: 'Incremental', value: 'Incremental' },
  { key: 'Batch', value: 'Batch' },
  { key: 'Streaming', value: 'Streaming' },
];

const ENTER_KEY = 'Enter';
export const filterItemTxtSize = '0.875rem';

export const StyledFormControlLabel = styled(FormControlLabel)({
  '& .MuiFormControlLabel-label': {
    backgroundColor: FILTER_PANEL_GREY,
    fontSize: filterItemTxtSize,
    marginBottom: 0,
    padding: 0,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    maxWidth: '110px',
  },
});

export const StyledAccordion = styled(Accordion)({
  backgroundColor: FILTER_PANEL_GREY,
  width: 150,
  boxShadow: 'none',
});

export const StyledDivider = styled(Divider)({
  width: '100%',
  backgroundColor: FEATUREFORM_DARK_GRAY,
});

export const BaseFilterPanel = ({
  onTextFieldEnter = () => null,
  children,
}) => {
  const [textFieldValue, setTextFieldValue] = useState('');

  const handleTextFieldChange = (event) => {
    setTextFieldValue(event.target.value);

    if (event.target.value === '') {
      onTextFieldEnter('');
    }
  };

  const handleTextFieldKeyPress = (event) => {
    if (event.key === ENTER_KEY) {
      onTextFieldEnter(textFieldValue);
    }
  };

  return (
    <Box
      sx={{
        flexDirection: 'column',
        backgroundColor: FILTER_PANEL_GREY,
        width: 240,
        minHeight: '100vh',
        paddingLeft: 3,
      }}
    >
      <Typography
        data-testid='filter-heading'
        variant='subtitle2'
        fontWeight='bold'
        marginTop={3}
      >
        Filters
      </Typography>
      <TextField
        data-testid='search-name'
        fullWidth
        label='Name'
        InputLabelProps={{ shrink: true }}
        variant='outlined'
        value={textFieldValue}
        onChange={handleTextFieldChange}
        onKeyDown={handleTextFieldKeyPress}
        sx={{
          marginTop: 3,
          width: 150,
          fontSize: filterItemTxtSize,
        }}
        size='small'
      />
      {children}
    </Box>
  );
};

export default BaseFilterPanel;
