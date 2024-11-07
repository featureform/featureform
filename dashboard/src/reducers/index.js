// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { combineReducers } from 'redux';
import { breadCrumbsReducer } from '../components/breadcrumbs';
import { entityPageReducer } from '../components/entitypage';
import {
  aggregateDropdownSliceReducer,
  exponentialTimeSliderReducer,
  metricsSelectSliceReducer,
} from '../components/entitypage/elements';
import { homePageReducer } from '../components/homepage';
import {
  currentFilterReducer,
  currentPageReducer,
  resourceReducer,
  tagReducer,
  variantReducer,
} from '../components/resource-list';
import { searchResultsReducer } from '../components/searchresults';

export default combineReducers({
  resourceList: resourceReducer,
  selectedVariant: variantReducer,
  selectedTags: tagReducer,
  homePageSections: homePageReducer,
  breadCrumbs: breadCrumbsReducer,
  entityPage: entityPageReducer,
  searchResults: searchResultsReducer,
  timeRange: exponentialTimeSliderReducer,
  metricsSelect: metricsSelectSliceReducer,
  aggregates: aggregateDropdownSliceReducer,
  currentPage: currentPageReducer,
  currentFilter: currentFilterReducer,
});
