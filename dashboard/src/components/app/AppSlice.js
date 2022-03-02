import { createSlice } from "@reduxjs/toolkit";
import { resourceTypes } from "api/resources";
import { views } from "./App.js";
import Resource from "api/resources/Resource.js";

const navSectionSlice = createSlice({
  name: "navSections",
  initialState: [
    {
      name: "Resources",
      items: [
        {
          type: Resource.PrimaryData,
          title: "Primary Data",
          icon: "source",
          path: "/primary-data",
          view: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["PRIMARY_DATA"],
          },
        },
        {
          type: Resource.Feature,
          title: "Features",
          icon: "file-code",
          path: "/features",
          view: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["FEATURE"],
          },
        },
        {
          type: Resource.TrainingSet,
          title: "training-sets",
          icon: "storage",
          path: "/training-sets",
          view: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["TRAINING_DATASET"],
          },
        },
        {
          type: Resource.Entity,
          title: "Entities",
          icon: "archive",
          path: "/entities",
          view: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["ENTITY"],
          },
        },
        {
          type: Resource.Label,
          title: "Labels",
          icon: "archive",
          path: "/labels",
          view: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["LABEL"],
          },
        },
        {
          type: Resource.Model,
          title: "Models",
          icon: "archive",
          path: "/models",
          view: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["MODEL"],
          },
        },
        {
          type: Resource.Provider,
          title: "Providers",
          icon: "device_hub",
          path: "/providers",
          vies: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["PROVIDER"],
          },
        },
        {
          type: Resource.User,
          title: "Users",
          icon: "person",
          path: "/users",
          vies: views["RESOURCE_LIST"],
          viewProps: {
            type: resourceTypes["USER"],
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
