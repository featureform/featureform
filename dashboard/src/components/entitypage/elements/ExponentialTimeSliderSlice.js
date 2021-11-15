import { createSlice } from "@reduxjs/toolkit";

const exponentialTimeSliderSlice = createSlice({
  name: "timeSlider",
  initialState: {
    timeRange: [1, 0],
  },
  reducers: {
    changeTime: (state, action) => {
      state.timeRange = action.payload.timeRange;
    },
  },
});

export const changeTime = exponentialTimeSliderSlice.actions.changeTime;

export default exponentialTimeSliderSlice.reducer;
