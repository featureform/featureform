// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { createSlice } from '@reduxjs/toolkit';
import Resource from '../../api/resources/Resource.js';
const reduceFn = (map, type) => {
  map[type] = null;
  return map;
};
const reduceFnInitial = {};
export const initialState = Resource.resourceTypes.reduce(
  reduceFn,
  reduceFnInitial
);

const filterSlice = createSlice({
  name: 'resourceFilter',
  initialState: initialState,
  reducers: {
    setCurrentFilter: (state, action) => {
      const { type, currentFilter } = action?.payload ?? {};
      let typePayload = type;
      if (Resource.typeToName[type]) {
        typePayload = Resource.typeToName[type];
      }
      if (typePayload in state) {
        state[typePayload] = currentFilter;
      }
    },
  },
});

export const setCurrentFilter = filterSlice.actions.setCurrentFilter;

export default filterSlice.reducer;
