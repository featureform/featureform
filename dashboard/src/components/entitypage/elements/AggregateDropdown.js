// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import Box from '@mui/material/Box';
import FormControl from '@mui/material/FormControl';
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import Select from '@mui/material/Select';
import React, { useState } from 'react';
import { connect } from 'react-redux';
import { changeAggregate } from './AggregateDropdownSlice.js';

const dropdownValues = [
  { label: 'Average', value: 'avg' },
  { label: 'Sum', value: 'sum' },
  { label: 'Minimum', value: 'min' },
  { label: 'Maximum', value: 'max' },
];

function AggregateDropdwon({ graph, aggregates, changeAggregate }) {
  const [agg, setAgg] = useState(aggregates.aggregates[graph]);

  const handleChange = (event) => {
    setAgg(event.target.value);
    changeAggregate(0, event.target.value);
  };

  return (
    <div>
      <Box sx={{ minWidth: 120 }}>
        <FormControl fullWidth>
          <InputLabel id='demo-simple-select-label'>
            Aggregation Select
          </InputLabel>
          <Select
            labelId='demo-simple-select-label'
            id='demo-simple-select'
            value={agg}
            label='Aggregation Select'
            onChange={handleChange}
          >
            {/* {Object.keys(resourceData).map((key, i) => ( */}
            {dropdownValues.map((item) => (
              <MenuItem key={item.label} value={item.value}>
                {item.label}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </Box>
    </div>
  );
}

function mapStateToProps(state) {
  return {
    aggregates: state.aggregates,
  };
}

const mapDispatchToProps = (dispatch) => {
  return {
    changeAggregate: (graph, aggregate) =>
      dispatch(changeAggregate({ graph, aggregate })),
  };
};

export default connect(mapStateToProps, mapDispatchToProps)(AggregateDropdwon);
