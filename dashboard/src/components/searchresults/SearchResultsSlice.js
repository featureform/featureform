// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';

export const fetchSearch = createAsyncThunk(
  'searchResults/fetchSearch',
  async ({ api, query }, { signal }) => {
    const response = await api.fetchSearch(query, signal);
    return response;
  }
);

export const initialState = {};

const searchResultsSlice = createSlice({
  name: 'searchResults',
  initialState: initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder
      .addCase(fetchSearch.pending, (state, action) => {
        const requestId = action.meta.requestId;
        state.requestId = requestId;
        state.resources = null;
        state.loading = true;
        state.failed = false;
      })
      .addCase(fetchSearch.fulfilled, (state, action) => {
        const requestId = action.meta.requestId;
        if (requestId !== state.requestId) {
          return;
        }
        state.resources = action?.payload;
        state.loading = false;
        state.failed = false;
      })
      .addCase(fetchSearch.rejected, (state, action) => {
        const requestId = action.meta.requestId;
        if (requestId !== state.requestId) {
          return;
        }
        state.loading = false;
        state.failed = true;
      });
  },
});

export default searchResultsSlice.reducer;
