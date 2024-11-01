import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
import {
  STATUS_OPTIONS, 
  StyledAccordion, 
  filterItemTxtSize, 
  StyledFormControlLabel,
  StyledDivider,
} from '../BaseFilterPanel';
import {
  AccordionDetails,
  AccordionSummary,
  Checkbox,
  FormGroup,
  Typography,
} from '@mui/material';
import React, { useState } from 'react';

export const FilterPanel = ({
  filters,
  tags = [],
  owners = [],
  labels = [],
  providers = [],
  onCheckBoxChange = () => null,
}) => {
  const [statusesExpanded, setStatusesExpanded] = useState(true);
  const [tagsExpanded, setTagsExpanded] = useState(false);
  const [ownerExpanded, setOwnerExpanded] = useState(false);
  const [labelsExpanded, setLabelsExpanded] = useState(false);
  const [providersExpanded, setProvidersExpanded] = useState(false);

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

      <StyledAccordion
        expanded={labelsExpanded}
        onChange={() => setLabelsExpanded(!labelsExpanded)}
        disableGutters={true}
      >
        <AccordionSummary expandIcon={<ArrowDropDownIcon />}>
          <Typography sx={{ fontSize: filterItemTxtSize }}>Labels</Typography>
        </AccordionSummary>
        <StyledDivider />
        <AccordionDetails>
          <FormGroup>
            {labels.map((label) => (
              <StyledFormControlLabel
                key={label}
                control={
                  <Checkbox
                    checked={filters.Labels?.includes(label) || false}
                    onChange={() => onCheckBoxChange('Labels', label)}
                    sx={{ '& .MuiSvgIcon-root': { fontSize: 25 } }}
                  />
                }
                label={label}
              />
            ))}
          </FormGroup>
        </AccordionDetails>
      </StyledAccordion>

      <StyledAccordion
        expanded={providersExpanded}
        onChange={() => setProvidersExpanded(!providersExpanded)}
        disableGutters={true}
      >
        <AccordionSummary expandIcon={<ArrowDropDownIcon />}>
          <Typography sx={{ fontSize: filterItemTxtSize }}>Providers</Typography>
        </AccordionSummary>
        <StyledDivider />
        <AccordionDetails>
          <FormGroup>
            {providers.map((provider) => (
              <StyledFormControlLabel
                key={provider}
                control={
                  <Checkbox
                    checked={filters.Providers?.includes(provider) || false}
                    onChange={() => onCheckBoxChange('Providers', provider)}
                    sx={{ '& .MuiSvgIcon-root': { fontSize: 25 } }}
                  />
                }
                label={provider}
              />
            ))}
          </FormGroup>
        </AccordionDetails>
      </StyledAccordion>
    </>
  );
};

export default FilterPanel;
