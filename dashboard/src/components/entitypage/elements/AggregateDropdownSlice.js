// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { createSlice } from '@reduxjs/toolkit';

const aggregateDropdownSlice = createSlice({
  name: 'aggregateDropdown',
  initialState: {
    aggregates: ['avg', 'avg', 'avg'],
  },
  reducers: {
    changeAggregate: (state, action) => {
      state.aggregates[action?.payload?.graph] = action?.payload?.aggregate;
    },
  },
});

export const changeAggregate = aggregateDropdownSlice.actions.changeAggregate;

export default aggregateDropdownSlice.reducer;
