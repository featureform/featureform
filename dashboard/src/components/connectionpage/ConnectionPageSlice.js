import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";

export const fetchStatus = createAsyncThunk(
  "metricsSelect/fetchStatus",
  async ({ api, resource }, { signal }) => {
    const response = await api.checkStatus(signal);
    return response;
  }
);
const connectionPageSlice = createSlice({
  name: "connectionStatus",
  initialState: {
    statuses: {},
  },
  reducers: {},
  extraReducers: {
    [fetchStatus.pending]: (state, action) => {
      state.statuses[action.meta.arg.resource] = "loading";
      const requestId = action.meta.requestId;
      state.requestId = requestId;
      state.resources = null;
      state.loading = true;
      state.failed = false;
    },
    [fetchStatus.fulfilled]: (state, action) => {
      state.statuses[action.meta.arg.resource] = "connected";

      state.loading = false;
      state.failed = false;
    },
    [fetchStatus.rejected]: (state, action) => {
      state.statuses[action.meta.arg.resource] = "failed";
      state.loading = false;
      state.failed = true;
    },
  },
});

export default connectionPageSlice.reducer;
