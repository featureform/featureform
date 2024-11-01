// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';
import Resource from '../../api/resources/Resource.js';

const assertAndCheck = (assertion, errorMessage) => {
  console.assert(assertion, { errorMsg: errorMessage });
  return assertion;
};

const isValidResponse = (resources, hasVariants) => {
  let af = true;
  af &= assertAndCheck(
    Array.isArray(resources),
    'Resource list fetch not an array'
  );

  if (hasVariants) {
    resources.forEach((resource) => {
      af &= assertAndCheck('name' in resource, 'Resource has no name element');
      af &= assertAndCheck(
        'default-variant' in resource,
        'Resource has no default variant'
      );
      af &= assertAndCheck(
        'all-variants' in resource,
        'Resource has no variants list'
      );
      af &= assertAndCheck(
        'variants' in resource,
        'Resource has no variants object'
      );
      resource['all-variants'].forEach((variant) => {
        af &= assertAndCheck(
          Object.keys(resource['variants']).includes(variant),
          'Element of variant list not in variants'
        );
      });
      af &= assertAndCheck(
        Object.keys(resource['variants']).includes(resource['default-variant']),
        'default variant not included in resource'
      );
      Object.keys(resource['variants']).forEach((key) => {
        af &= assertAndCheck(
          resource['all-variants'].includes(key),
          'Variant in variant object not in variant list'
        );
      });
    });
  } else {
    resources.forEach((resource) => {
      af &= assertAndCheck('name' in resource, 'Resource has no name element');
    });
  }

  return af;
};

export const fetchResources = createAsyncThunk(
  'resourceList/fetchByType',
  async ({ api, type, pageSize, offset, filter }, { signal }) => {
    const response = await api.fetchResources(
      type,
      pageSize,
      offset,
      filter,
      signal
    );
    return response;
  },
  {
    condition: ({ type }, { getState }) => {
      const { loading } = getState().resourceList[type];
      if (loading) {
        return false;
      }
    },
  }
);

const reduceFn = (map, type) => {
  map[type] = {};
  return map;
};
const reduceFnInitial = { selectedType: null };
export const initialState = Resource.resourceTypes.reduce(
  reduceFn,
  reduceFnInitial
);

const resourceSlice = createSlice({
  name: 'resourceList',
  initialState: initialState,
  reducers: {
    setCurrentType: (state, action) => {
      state.selectedType = action?.payload?.selectedType;
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(fetchResources.pending, (state, action) => {
        const type = action.meta.arg.type;
        const requestId = action.meta.requestId;
        state[type].requestId = requestId;
        state[type].resources = null;
        state[type].loading = true;
        state[type].failed = false;
      })
      .addCase(fetchResources.fulfilled, (state, action) => {
        const type = action.meta.arg.type;
        const requestId = action.meta.requestId;
        if (requestId !== state[type].requestId) {
          return;
        }

        let hasRequired = true;
        if (action?.payload?.data?.length > 0) {
          hasRequired = false;
          const resourceType = action?.payload?.data[0]?.type;
          const hasVariants = resourceType.hasVariants;
          hasRequired = isValidResponse(action?.payload?.data, hasVariants);
        }
        if (hasRequired) {
          state[type].resources = action?.payload?.data;
          state[type].count = action?.payload?.count;
          state[type].loading = false;
          state[type].failed = false;
        } else {
          state[type].resources = null;
          state[type].count = 0;
          state[type].loading = false;
          state[type].failed = true;
        }
      })
      .addCase(fetchResources.rejected, (state, action) => {
        const type = action.meta.arg.type;
        const requestId = action.meta.requestId;
        if (requestId !== state[type].requestId) {
          return;
        }
        state[type].loading = false;
        state[type].failed = true;
      });
  },
});

export const { setCurrentType } = resourceSlice.actions;

export default resourceSlice.reducer;
