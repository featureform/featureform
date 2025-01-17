// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { createSlice } from '@reduxjs/toolkit';

const homePageSlice = createSlice({
  name: 'homePageSections',
  initialState: {
    features: [
      {
        type: 'Provider',
        disabled: false,
      },
      {
        type: 'Source',
        disabled: false,
      },
      {
        type: 'Feature',
        disabled: false,
      },
      {
        type: 'Label',
        disabled: false,
      },
      {
        type: 'Entity',
        disabled: false,
      },
      {
        type: 'TrainingSet',
        disabled: false,
      },
      {
        type: 'Model',
        disabled: false,
      },
    ],
  },
});

export default homePageSlice.reducer;
