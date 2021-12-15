import { createSlice } from "@reduxjs/toolkit";

const homePageSlice = createSlice({
  name: "homePageSections",
  initialState: {
    features: [
      {
        title: "Datasets",
        icon: "storage",
        path: "/datasets",
        disabled: false,
        description: "Training datasets",
      },
      {
        title: "Materialized Views",
        icon: "workspaces",
        path: "/materialized-views",
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
        title: "Feature Sets",
        icon: "account_tree",
        path: "/feature-sets",
        disabled: false,
        description: "Not avaliable",
      },
    ],
  },
});

export default homePageSlice.reducer;
