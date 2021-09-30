import { createSlice } from "@reduxjs/toolkit";

const searchBarSlice = createSlice({
  name: "searchQuery",
  initialState: "",
  reducers: {
    setQuery: (state, action) => {
      state = action.payload;
    },
  },
});

export const { setQuery } = searchBarSlice.actions;

export default searchBarSlice.reducer;
