// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { createSlice } from '@reduxjs/toolkit';

const exponentialTimeSliderSlice = createSlice({
  name: 'timeSlider',
  initialState: {
    timeRange: [60, 0],
  },
  reducers: {
    changeTime: (state, action) => {
      state.timeRange = action?.payload?.timeRange;
    },
  },
});

export const changeTime = exponentialTimeSliderSlice.actions.changeTime;

export default exponentialTimeSliderSlice.reducer;
