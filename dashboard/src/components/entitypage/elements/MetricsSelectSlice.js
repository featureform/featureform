// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';

export const fetchMetrics = createAsyncThunk(
  'metricsSelect/fetchMetrics',
  async ({ api }, { signal }) => {
    const instance = '';
    const response = await api.fetchMetrics(instance, signal);
    return response;
  },
  {
    condition: (_, { getState }) => {
      const { loading } = getState().metricsSelect;
      if (loading) {
        return false;
      }
    },
  }
);

const metricsSelectSlice = createSlice({
  name: 'metricsSelect',
  initialState: {
    instances: [],
    metrics: 0,
  },
  reducers: {
    modifyInstances: (state, action) => {
      state.instances = action?.payload?.instances;
    },
    modifyMetrics: (state, action) => {
      state.metrics = action?.payload?.selection;
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(fetchMetrics.pending, (state, action) => {
        const requestId = action.meta.requestId;
        state.requestId = requestId;
        state.resources = null;
        state.loading = true;
        state.failed = false;
      })
      .addCase(fetchMetrics.fulfilled, (state, action) => {
        const requestId = action.meta.requestId;
        if (requestId !== state.requestId) {
          return;
        }
        state.resources = action?.payload?.data;
        state.loading = false;
        state.failed = false;
      })
      .addCase(fetchMetrics.rejected, (state, action) => {
        const requestId = action.meta.requestId;
        if (requestId !== state.requestId) {
          return;
        }
        state.loading = false;
        state.failed = true;
      });
  },
});

export const modifyInstances = metricsSelectSlice.actions.modifyInstances;
export const modifyMetrics = metricsSelectSlice.actions.modifyMetrics;

export default metricsSelectSlice.reducer;
