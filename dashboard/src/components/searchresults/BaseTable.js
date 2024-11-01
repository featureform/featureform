// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  Checkbox,
  FormControlLabel,
  FormGroup,
  Typography,
} from '@mui/material';
import { styled } from '@mui/system';
import { DataGrid } from '@mui/x-data-grid';
import React, { useState } from 'react';
import { deepCopy } from '../../helper';

const defaultState = Object.freeze({
  All: { checked: true, types: [''] },
  'Training Sets': {
    checked: false,
    types: ['TRAINING_SET', 'TRAINING_SET_VARIANT'],
  },
  Features: { checked: false, types: ['FEATURE', 'FEATURE_VARIANT'] },
  Labels: { checked: false, types: ['LABEL', 'LABEL_VARIANT'] },
  Entities: { checked: false, types: ['ENTITY', 'USER'] },
  Models: { checked: false, types: ['MODEL'] },
  Datasets: { checked: false, types: ['SOURCE', 'SOURCE_VARIANT'] },
  Providers: { checked: false, types: ['PROVIDER'] },
});

const MainContainer = styled(Box)({
  display: 'flex',
});

const GridContainer = styled(Box)({
  width: '100%',
});

const StyledDataGrid = styled(DataGrid)({
  '& .MuiDataGrid-cell:focus': {
    outline: 'none',
  },
  '& .MuiDataGrid-columnHeader:focus': {
    outline: 'none',
  },
  '& .MuiDataGrid-colCellWrapper': {
    display: 'none',
  },
  marginBottom: '1.5em',
  cursor: 'pointer',
  '& .MuiDataGrid-columnHeaderTitle': {
    fontWeight: 'bold',
  },
});

const StyledAccordion = styled(Accordion)({
  width: 215,
  marginRight: 2,
});

export const BaseTable = ({
  title = '',
  columns = [],
  rows = [],
  redirect = () => null,
}) => {
  const [checkboxMap, setCheckboxMap] = useState(deepCopy(defaultState));

  const viewAll = () => {
    const updateMap = deepCopy(defaultState);
    setCheckboxMap(updateMap);
  };

  const setCheckbox = (item = '') => {
    if (item in checkboxMap) {
      const updateMap = { ...checkboxMap };
      updateMap.All.checked = false;
      updateMap[item].checked = !updateMap[item].checked;

      const allUnchecked = Object.values(updateMap).every(
        (item) => item.checked === false
      );

      if (allUnchecked) {
        viewAll();
      } else {
        setCheckboxMap(updateMap);
      }
    }
  };

  const filteredRows = rows?.filter((row) => {
    let result = true;
    if (checkboxMap.All.checked) {
      result = true;
    } else {
      const typeList = [];
      Object.values(checkboxMap).forEach((item) => {
        if (item.checked === true) {
          typeList.push(...item.types);
        }
      });
      result = typeList.includes(row.Type);
    }
    return result;
  });

  return (
    <>
      <h4>{title}</h4>
      <MainContainer>
        <StyledAccordion expanded>
          <AccordionSummary expandIcon={<ArrowDropDownIcon />}>
            <Typography>Resource Type</Typography>
          </AccordionSummary>
          <AccordionDetails>
            <FormGroup>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={checkboxMap.All.checked}
                    onClick={viewAll}
                  />
                }
                label='All'
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={checkboxMap['Training Sets'].checked}
                    onClick={() => setCheckbox('Training Sets')}
                  />
                }
                label='Training Sets'
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={checkboxMap.Features.checked}
                    onClick={() => setCheckbox('Features')}
                  />
                }
                label='Features'
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={checkboxMap.Labels.checked}
                    onClick={() => setCheckbox('Labels')}
                  />
                }
                label='Labels'
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={checkboxMap.Entities.checked}
                    onClick={() => setCheckbox('Entities')}
                  />
                }
                label='Entities'
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={checkboxMap.Models.checked}
                    onClick={() => setCheckbox('Models')}
                  />
                }
                label='Models'
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={checkboxMap.Datasets.checked}
                    onClick={() => setCheckbox('Datasets')}
                  />
                }
                label='Datasets'
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={checkboxMap.Providers.checked}
                    onClick={() => setCheckbox('Providers')}
                  />
                }
                label='Providers'
              />
            </FormGroup>
          </AccordionDetails>
        </StyledAccordion>
        <GridContainer>
          <StyledDataGrid
            disableVirtualization
            autoHeight
            aria-label={title}
            rows={filteredRows}
            columns={columns}
            rowHeight={65}
            hideFooterSelectedRowCount
            disableColumnFilter
            disableColumnMenu
            disableColumnSelector
            initialState={{
              pagination: { paginationModel: { page: 0, pageSize: 10 } },
              sorting: [{ field: 'Name', sort: 'desc' }],
            }}
            pageSize={10}
            rowsPerPageOptions={[10]}
            onRowClick={(params, event) => {
              event?.preventDefault();
              if (params?.row?.Name) {
                redirect(params.row.Name, params.row.Variant, params.row.Type);
              }
            }}
            getRowId={(row) => row.Name + row.Variant}
          />
        </GridContainer>
      </MainContainer>
    </>
  );
};

export default BaseTable;
