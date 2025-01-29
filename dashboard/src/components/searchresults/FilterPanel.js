// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
import {
  AccordionDetails,
  AccordionSummary,
  Checkbox,
  FormGroup,
  Typography,
  Accordion,
  Divider,
  FormControlLabel,
  Box,
} from '@mui/material';
import React, { useState } from 'react';
import { styled } from '@mui/system';
import { FEATUREFORM_DARK_GRAY } from '../../styles/theme';

const FILTER_PANEL_GREY = "#F9F9FA";

const STATUS_OPTIONS = [
    { key: 'No Status', value: 'NO_STATUS' },
    { key: 'Created', value: 'CREATED' },
    { key: 'Pending', value: 'PENDING' },
    { key: 'Ready', value: 'READY' },
    { key: 'Failed', value: 'FAILED' },
    { key: 'Running', value: 'RUNNING' },
    { key: 'Cancelled', value: 'CANCELLED' },
];

const filterItemTxtSize = '0.875rem';

const StyledFormControlLabel = styled(FormControlLabel)({
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

const StyledAccordion = styled(Accordion)({
  backgroundColor: FILTER_PANEL_GREY,
  width: 150,
  boxShadow: 'none',
});

const StyledDivider = styled(Divider)({
  width: '100%',
  backgroundColor: FEATUREFORM_DARK_GRAY
});

const FilterPanel = ({
  filters,
  onCheckBoxChange = () => null,
}) => {
  const [statusesExpanded, setStatusesExpanded] = useState(true);

  return (
    <Box sx={{ flexDirection: 'column', backgroundColor: FILTER_PANEL_GREY, width: 240, minHeight: '100vh', paddingLeft: 3}}>
      <Typography data-testid='filter-heading' variant='subtitle2' fontWeight='bold' marginTop={3}>
        Filters
      </Typography>
      <StyledAccordion
        expanded={statusesExpanded}
        onChange={() => setStatusesExpanded(!statusesExpanded)}
        disableGutters={true}
      >
        <AccordionSummary expandIcon={<ArrowDropDownIcon />}>
          <Typography sx={{ fontSize: filterItemTxtSize }}>Status</Typography>
        </AccordionSummary>
        <StyledDivider />
        <AccordionDetails>
          <FormGroup>
            {STATUS_OPTIONS.map((status) => (
              <StyledFormControlLabel
                key={status.key}
                control={
                  <Checkbox
                    checked={filters?.Statuses?.includes(status.value) || false}
                    onChange={() => onCheckBoxChange('Statuses', status.value)}
                    sx={{ '& .MuiSvgIcon-root': { fontSize: 25 } }}
                  />
                }
                label={status.key}
              />
            ))}
          </FormGroup>
        </AccordionDetails>
      </StyledAccordion>

      </Box>
  );
};

export default FilterPanel;
