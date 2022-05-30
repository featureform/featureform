import { combineReducers } from "redux";
import {
  resourceReducer,
  variantReducer,
  tagReducer,
} from "components/resource-list";
import { homePageReducer } from "components/homepage";
import { breadCrumbsReducer } from "components/breadcrumbs";
import { entityPageReducer } from "components/entitypage";
import { searchResultsReducer } from "components/searchresults";
import { exponentialTimeSliderReducer } from "components/entitypage/elements";
import { metricsSelectSliceReducer } from "components/entitypage/elements";
import { connectionPageSliceReducer } from "components/connectionpage";
import { aggregateDropdownSliceReducer } from "components/entitypage/elements";

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
