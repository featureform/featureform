import { createSlice } from "@reduxjs/toolkit";

const breadCrumbsSlice = createSlice({
  name: "breadCrumbs",
  initialState: [
    {
      path: [],
    },
  ],
});

export default breadCrumbsSlice.reducer;
