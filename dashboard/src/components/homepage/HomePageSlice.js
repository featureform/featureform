import { createSlice } from "@reduxjs/toolkit";
import Resource from "api/resources/Resource.js";

const homePageSlice = createSlice({
  name: "homePageSections",
  initialState: {
    features: [
      {
        type: Resource.TrainingSet,
        disabled: false,
      },
      {
        type: Resource.Feature,
        disabled: false,
      },
      {
        type: Resource.Entity,
        disabled: false,
      },
      {
        type: Resource.Label,
        disabled: false,
      },
      {
        type: Resource.Model,
        disabled: false,
      },
      {
        type: Resource.PrimaryData,
        disabled: false,
      },
      {
        type: Resource.Provider,
        disabled: false,
      },
      {
        type: Resource.User,
        disabled: false,
      },
    ],
  },
});

export default homePageSlice.reducer;
