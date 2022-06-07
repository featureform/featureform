import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";

export const fetchSearch = createAsyncThunk(
  "searchResults/fetchSearch",
  async ({ api, query }, { signal }) => {
    const response = await api.fetchSearch(query, signal);
    return response;
  }
);

export const initialState = {};

const searchResultsSlice = createSlice({
  name: "searchResults",
  // initialState is a map between each resource type to an empty object.
  initialState: initialState,
  extraReducers: (builder) => {
    builder.addCase(fetchSearch.fulfilled, (state, action) => {
      // Add user to the state array
      const requestId = action.meta.requestId;
      state.requestId = requestId;
      state.loading = false;
      state.failed = false;
      state.resources = action.payload;
    })
    // [fetchSearch.pending]: (state, action) => {
    //   const requestId = action.meta.requestId;
    //   state.requestId = requestId;
    //   state.resources = null;
    //   state.loading = true;
    //   state.failed = false;
    // },
    // [fetchSearch.fulfilled]: (state, action) => {
    //   const requestId = action.meta.requestId;
    //   if (requestId !== state.requestId) {
    //     return;
    //   }
    //   state.resources = action.payload;
    //   state.loading = false;
    //   state.failed = false;
    // },
    // [fetchSearch.rejected]: (state, action) => {
    //   const requestId = action.meta.requestId;
    //   if (requestId !== state.requestId) {
    //     return;
    //   }
    //   state.loading = false;
    //   state.failed = true;
    // },
  },
});

export default searchResultsSlice.reducer;
