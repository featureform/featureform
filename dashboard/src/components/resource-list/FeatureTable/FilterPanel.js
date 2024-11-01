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
  STATUS_OPTIONS,
  StyledAccordion,
  StyledDivider,
  StyledFormControlLabel,
} from '../BaseFilterPanel';

export const FilterPanel = ({
  filters,
  tags = [],
  owners = [],
  onCheckBoxChange = () => null,
}) => {
  const [statusesExpanded, setStatusesExpanded] = useState(true);
  const [tagsExpanded, setTagsExpanded] = useState(false);
  const [ownerExpanded, setOwnerExpanded] = useState(false);

  return (
    <>
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
                    checked={filters.Statuses?.includes(status.value) || false}
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

      <StyledAccordion
        expanded={ownerExpanded}
        onChange={() => setOwnerExpanded(!ownerExpanded)}
        disableGutters={true}
      >
        <AccordionSummary expandIcon={<ArrowDropDownIcon />}>
          <Typography sx={{ fontSize: filterItemTxtSize }}>Owners</Typography>
        </AccordionSummary>
        <StyledDivider />
        <AccordionDetails>
          <FormGroup>
            {owners.map((owner) => (
              <StyledFormControlLabel
                key={owner.name}
                control={
                  <Checkbox
                    checked={filters.Owners?.includes(owner.name) || false}
                    onChange={() => onCheckBoxChange('Owners', owner.name)}
                    sx={{ '& .MuiSvgIcon-root': { fontSize: 25 } }}
                  />
                }
                label={owner.name}
              />
            ))}
          </FormGroup>
        </AccordionDetails>
      </StyledAccordion>

      <StyledAccordion
        expanded={tagsExpanded}
        onChange={() => setTagsExpanded(!tagsExpanded)}
        disableGutters={true}
      >
        <AccordionSummary expandIcon={<ArrowDropDownIcon />}>
          <Typography sx={{ fontSize: filterItemTxtSize }}>Tags</Typography>
        </AccordionSummary>
        <StyledDivider />
        <AccordionDetails>
          <FormGroup>
            {tags.map((tag) => (
              <StyledFormControlLabel
                key={tag.name}
                control={
                  <Checkbox
                    checked={filters.Tags?.includes(tag.name) || false}
                    onChange={() => onCheckBoxChange('Tags', tag.name)}
                    sx={{ '& .MuiSvgIcon-root': { fontSize: 25 } }}
                  />
                }
                label={tag.name}
              />
            ))}
          </FormGroup>
        </AccordionDetails>
      </StyledAccordion>
    </>
  );
};

export default FilterPanel;
