// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { configureStore } from '@reduxjs/toolkit';
import rootReducer from '../../../reducers';

const store = configureStore({
  reducer: rootReducer,
});

export const newTestStore = () =>
  configureStore({
    reducer: rootReducer,
  });

export default store;
