import { createSlice } from "@reduxjs/toolkit";

const aggregateDropdownSlice = createSlice({
  name: "aggregateDropdown",
  initialState: {
    aggregates: ["avg", "avg", "avg"],
  },
  reducers: {
    changeAggregate: (state, action) => {
      state.aggregates[action.payload.graph] = action.payload.aggregate;
    },
  },
});

export const changeAggregate = aggregateDropdownSlice.actions.changeAggregate;

export default aggregateDropdownSlice.reducer;
