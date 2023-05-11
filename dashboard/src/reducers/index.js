import { combineReducers } from 'redux';
import { breadCrumbsReducer } from '../components/breadcrumbs';
import { connectionPageSliceReducer } from '../components/connectionpage';
import { entityPageReducer } from '../components/entitypage';
import {
  aggregateDropdownSliceReducer,
  exponentialTimeSliderReducer,
  metricsSelectSliceReducer,
} from '../components/entitypage/elements';
import { homePageReducer } from '../components/homepage';
import {
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
  connectionStatus: connectionPageSliceReducer,
  aggregates: aggregateDropdownSliceReducer,
});
