import { createSlice } from "@reduxjs/toolkit";
import Resource from "api/resources/Resource.js";
const reduceFn = (map, type) => {
  map[type] = {};
  return map;
};
const reduceFnInitial = {};
export const initialState = Resource.resourceTypes.reduce(
  reduceFn,
  reduceFnInitial
);

const versionSlice = createSlice({
  name: "resourceVersion",
  // initialState is a map between each resource type to an empty object.
  initialState: initialState,
  reducers: {
    set: (state, action) => {
      const { type, name, version } = action.payload;
      state[type][name] = version;
    },
  },
});

export const setVersion = versionSlice.actions.set;

export default versionSlice.reducer;
