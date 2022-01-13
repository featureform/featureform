import { createSlice } from "@reduxjs/toolkit";

const homePageSlice = createSlice({
  name: "homePageSections",
  initialState: {
    features: [
      {
        title: "Training Datasets",
        icon: "storage",
        path: "/training-datasets",
        disabled: false,
        description: "Training datasets",
      },
      {
        title: "Transformations",
        icon: "workspaces",
        path: "/transformations",
        disabled: false,
        description: "Logical queries of entity data",
      },
      {
        title: "Features",
        icon: "description",
        path: "/features",
        disabled: false,
        description: "Historical feature repository",
      },
      {
        title: "Entities",
        icon: "fingerprint",
        path: "/entities",
        disabled: false,
        description: "Not avaliable",
      },
      {
        title: "Labels",
        icon: "label",
        path: "/labels",
        disabled: false,
        description: "Not avaliable",
      },
      {
        title: "Models",
        icon: "model_training",
        path: "/models",
        disabled: false,
        description: "Not avaliable",
      },
      {
        title: "Data Sources",
        icon: "source",
        path: "/data-sources",
        disabled: false,
        description: "Not avaliable",
      },
      {
        title: "Providers",
        icon: "device_hub",
        path: "/providers",
        disabled: false,
        description: "Not avaliable",
      },
      {
        title: "Users",
        icon: "person",
        path: "/users",
        disabled: false,
        description: "Not avaliable",
      },
    ],
  },
});

export default homePageSlice.reducer;
