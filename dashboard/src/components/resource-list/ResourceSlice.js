import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import Resource from "api/resources/Resource.js";

const assertAndCheck = (assertion, errorMessage) => {
  console.assert(assertion, { errorMsg: errorMessage });
  return assertion;
};

const isValidResponse = (resources, hasVariants) => {
  let af = true;
  af &= assertAndCheck(
    Array.isArray(resources),
    "Resource list fetch not an array"
  );

  if (hasVariants) {
    resources.forEach((resource) => {
      af &= assertAndCheck("name" in resource, "Resource has no name element");
      af &= assertAndCheck(
        "default-variant" in resource,
        "Resource has no default variant"
      );
      af &= assertAndCheck(
        "all-variants" in resource,
        "Resource has no variants list"
      );
      af &= assertAndCheck(
        "variants" in resource,
        "Resource has no variants object"
      );
      resource["all-variants"].forEach((variant) => {
        af &= assertAndCheck(
          Object.keys(resource["variants"]).includes(variant),
          "Element of variant list not in variants"
        );
      });
      af &= assertAndCheck(
        Object.keys(resource["variants"]).includes(resource["default-variant"]),
        "default variant not included in resource"
      );
      Object.keys(resource["variants"]).forEach((key) => {
        af &= assertAndCheck(
          resource["all-variants"].includes(key),
          "Variant in variant object not in variant list"
        );
      });
    });
  } else {
    resources.forEach((resource) => {
      af &= assertAndCheck("name" in resource, "Resource has no name element");
    });
  }

  return af;
};

export const fetchResources = createAsyncThunk(
  "resourceList/fetchByType",
  async ({ api, type }, { signal }) => {
    const response = await api.fetchResources(type, signal);
    return response.data;
  },
  {
    condition: ({ type }, { getState }) => {
      const { resources, loading } = getState().resourceList[type];

      if (loading || resources) {
        return false;
      }
    },
  }
);

const reduceFn = (map, type) => {
  map[type] = {};
  return map;
};
const reduceFnInitial = {};
export const initialState = Resource.resourceTypes.reduce(
  reduceFn,
  reduceFnInitial
);

const resourceSlice = createSlice({
  name: "resourceList",
  // initialState is a map between each resource type to an empty object.
  initialState: initialState,
  extraReducers: {
    [fetchResources.pending]: (state, action) => {
      const type = action.meta.arg.type;
      const requestId = action.meta.requestId;
      state[type].requestId = requestId;
      state[type].resources = null;
      state[type].loading = true;
      state[type].failed = false;
    },
    [fetchResources.fulfilled]: (state, action) => {
      const type = action.meta.arg.type;
      const requestId = action.meta.requestId;
      if (requestId !== state[type].requestId) {
        return;
      }

      let hasRequired = false;
      if (action.payload.length > 0) {
        const resourceType = action.payload[0].type;
        const hasVariants = resourceType.hasVariants;
        hasRequired = isValidResponse(action.payload, hasVariants);
      }

      if (hasRequired) {
        state[type].resources = action.payload;
        state[type].loading = false;
        state[type].failed = false;
      } else {
        state[type].resources = null;
        state[type].loading = false;
        state[type].failed = true;
      }
    },
    [fetchResources.rejected]: (state, action) => {
      const type = action.meta.arg.type;
      const requestId = action.meta.requestId;
      if (requestId !== state[type].requestId) {
        return;
      }
      state[type].loading = false;
      state[type].failed = true;
    },
  },
});

export default resourceSlice.reducer;
