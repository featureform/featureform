import { createSlice } from "@reduxjs/toolkit";
import Resource from "api/resources/Resource.js";

const homePageSlice = createSlice({
  name: "homePageSections",
  initialState: {
    features: [
      {
        type: "TrainingSet",
        disabled: false,
      },
      {
        type: "Feature",
        disabled: false,
      },
      {
        type: "Entity",
        disabled: false,
      },
      {
        type: "Label",
        disabled: false,
      },
      {
        type: "Model",
        disabled: false,
      },
      {
        type: "PrimaryData",
        disabled: false,
      },
      {
        type: "Provider",
        disabled: false,
      },
      {
        type: "User",
        disabled: false,
      },
    ],
  },
});

export default homePageSlice.reducer;
