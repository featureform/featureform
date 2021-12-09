import { createSlice } from "@reduxjs/toolkit";
import { resourceTypes } from "api/resources";
import { views } from "./App.js";

const navSectionSlice = createSlice({
  name: "navSections",
  initialState: [
    {
      name: "Resources",
      items: [
        {
          title: "Data Sources",
          icon: "file-import",
          path: "/sources",
          view: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["DATA_SOURCE"],
          },
        },
        {
          title: "Materialized Views",
          icon: "copy",
          path: "/materialized-views",
          view: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["MATERIALIZED_VIEW"],
          },
        },
        {
          title: "Features",
          icon: "file-code",
          path: "/features",
          view: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["FEATURE"],
          },
        },
        {
          title: "Feature Sets",
          icon: "sitemap",
          path: "/feature-sets",
          view: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["FEATURE_SET"],
          },
        },
        {
          title: "Training Sets",
          icon: "archive",
          path: "/training-sets",
          view: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["TRAINING_SET"],
          },
        },
        {
          title: "Entities",
          icon: "archive",
          path: "/entities",
          view: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["ENTITY"],
          },
        },
        {
          title: "Labels",
          icon: "archive",
          path: "/labels",
          view: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["LABEL"],
          },
        },
        {
          title: "Models",
          icon: "archive",
          path: "/models",
          view: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["MODEL"],
          },
        },
        {
          title: "Spaces",
          icon: "spaces",
          path: "/spaces",
          vies: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["SPACE"],
          },
        },
      ],
    },
    {
      name: "Monitoring",
      items: [
        {
          title: "Metrics",
          icon: "chart-line",
          path: "/metrics",
          view: views["EMPTY"],
        },
        {
          title: "Deployment",
          icon: "server",
          path: "/deployment",
          view: views["EMPTY"],
        },
      ],
    },
    {
      name: "Admin",
      items: [
        { title: "Users", icon: "users", path: "/users", view: views["EMPTY"] },
        {
          title: "Settings",
          icon: "cogs",
          path: "/settings",
          view: views["EMPTY"],
        },
        {
          title: "Billing",
          icon: "wallet",
          path: "/billing",
          view: views["EMPTY"],
        },
        {
          title: "Documentation",
          icon: "book",
          path: "https://docs.streamsql.io",
          external: true,
        },
        {
          title: "Help",
          icon: "question",
          path: "/help",
          view: views["EMPTY"],
        },
      ],
    },
  ],
});

export default navSectionSlice.reducer;
