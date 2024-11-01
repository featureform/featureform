// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { createSlice } from '@reduxjs/toolkit';

const searchBarSlice = createSlice({
  name: 'searchQuery',
  initialState: '',
  reducers: {
    setQuery: (state, action) => {
      state = action?.payload;
    },
  },
});

export const { setQuery } = searchBarSlice.actions;

export default searchBarSlice.reducer;
