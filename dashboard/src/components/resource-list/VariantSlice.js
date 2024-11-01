// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { createSlice } from '@reduxjs/toolkit';
import Resource from '../../api/resources/Resource.js';
const reduceFn = (map, type) => {
  map[type] = {};
  return map;
};
const reduceFnInitial = {};
export const initialState = Resource.resourceTypes.reduce(
  reduceFn,
  reduceFnInitial
);

const variantSlice = createSlice({
  name: 'resourceVariant',
  // initialState is a map between each resource type to an empty object.
  initialState: initialState,
  reducers: {
    set: (state, action) => {
      const { type, name, variant } = action?.payload ?? {};
      let typePayload = type;
      if (Resource.typeToName[type]) {
        typePayload = Resource.typeToName[type];
      }
      if (state[typePayload]) {
        state[typePayload][name] = variant;
      }
    },
  },
});

export const setVariant = variantSlice.actions.set;

export default variantSlice.reducer;
