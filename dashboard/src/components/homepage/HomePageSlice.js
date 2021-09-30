import { createSlice } from "@reduxjs/toolkit";

const homePageSlice = createSlice({
  name: "homePageSections",
  initialState: {
    features: [
      {
        title: "Spaces",
        icon: "workspaces",
        path: "/spaces",
        disabled: false,
        description: "Dimensional space to store embeddings",
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
        disabled: true,
        description: "Not avaliable",
      },
      {
        title: "Labels",
        icon: "label",
        path: "/labels",
        disabled: true,
        description: "Not avaliable",
      },
      {
        title: "Feature Sets",
        icon: "account_tree",
        path: "/feature-sets",
        disabled: true,
        description: "Not avaliable",
      },
      {
        title: "Models",
        icon: "model_training",
        path: "/models",
        disabled: true,
        description: "Not avaliable",
      },
    ],
  },
});

export default homePageSlice.reducer;
