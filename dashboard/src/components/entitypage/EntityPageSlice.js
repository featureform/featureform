// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';

export const fetchEntity = createAsyncThunk(
  'entityPage/fetchByTitle',
  async ({ api, type, title }, { signal }) => {
    const response = await api.fetchEntity(type, title, signal);
    return response;
  }
);

const entityPageSlice = createSlice({
  name: 'entityPage',
  initialState: {},
  extraReducers: (builder) => {
    builder
      .addCase(fetchEntity.pending, (state, action) => {
        const requestId = action.meta.requestId;
        state.requestId = requestId;
        state.resources = null;
        state.loading = true;
        state.failed = false;
      })
      .addCase(fetchEntity.fulfilled, (state, action) => {
        const requestId = action.meta.requestId;
        if (requestId !== state.requestId) {
          return;
        }
        state.resources = action?.payload?.data;
        state.loading = false;
        state.failed = false;
      })
      .addCase(fetchEntity.rejected, (state, action) => {
        const requestId = action.meta.requestId;
        if (requestId !== state.requestId) {
          return;
        }
        state.loading = false;
        state.failed = true;
      });
  },
});

export default entityPageSlice.reducer;
