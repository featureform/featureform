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
} from '@mui/material';
import React, { useState } from 'react';
import {
  filterItemTxtSize,
  StyledAccordion,
  StyledDivider,
  StyledFormControlLabel,
} from '../BaseFilterPanel';

const PROVIDER_TYPES = [
  { key: 'Online', value: 'online' },
  { key: 'Offline', value: 'offline' },
  { key: 'File', value: 'file' },
];

const PROVIDER_STATUS_OPTIONS = [
  { key: 'Connected', value: 'connected' },
  { key: 'Disconnected', value: 'disconnected' },
];

export const FilterPanel = ({ filters, onCheckBoxChange = () => null }) => {
  const [statusExpanded, setStatusExpanded] = useState(true);
  const [typesExpanded, setTypesExpanded] = useState(true);

  return (
    <>
      <StyledAccordion
        expanded={typesExpanded}
        onChange={() => setTypesExpanded(!typesExpanded)}
        disableGutters={true}
      >
        <AccordionSummary expandIcon={<ArrowDropDownIcon />}>
          <Typography sx={{ fontSize: filterItemTxtSize }}>
            Provider Type
          </Typography>
        </AccordionSummary>
        <StyledDivider />
        <AccordionDetails>
          <FormGroup>
            {PROVIDER_TYPES.map((providerType) => (
              <StyledFormControlLabel
                key={providerType.key}
                control={
                  <Checkbox
                    checked={
                      filters.ProviderType?.includes(providerType.value) ??
                      false
                    }
                    onChange={() =>
                      onCheckBoxChange('ProviderType', providerType.value)
                    }
                    sx={{ '& .MuiSvgIcon-root': { fontSize: 25 } }}
                  />
                }
                label={providerType.key}
              />
            ))}
          </FormGroup>
        </AccordionDetails>
      </StyledAccordion>
      <StyledAccordion
        expanded={statusExpanded}
        onChange={() => setStatusExpanded(!statusExpanded)}
        disableGutters={true}
      >
        <AccordionSummary expandIcon={<ArrowDropDownIcon />}>
          <Typography sx={{ fontSize: filterItemTxtSize }}>Status</Typography>
        </AccordionSummary>
        <StyledDivider />
        <AccordionDetails>
          <FormGroup>
            {PROVIDER_STATUS_OPTIONS.map((status) => (
              <StyledFormControlLabel
                key={status.key}
                control={
                  <Checkbox
                    checked={filters.Status?.includes(status.value) ?? false}
                    onChange={() => onCheckBoxChange('Status', status.value)}
                    sx={{ '& .MuiSvgIcon-root': { fontSize: 25 } }}
                  />
                }
                label={status.key}
              />
            ))}
          </FormGroup>
        </AccordionDetails>
      </StyledAccordion>
    </>
  );
};

export default FilterPanel;
