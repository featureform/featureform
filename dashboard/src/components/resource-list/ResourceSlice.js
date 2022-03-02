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
        "all-versions" in resource,
        "Resource has no versions list"
      );
      af &= assertAndCheck(
        "versions" in resource,
        "Resource has no versions object"
      );
      resource["all-versions"].forEach((version) => {
        af &= assertAndCheck(
          Object.keys(resource["versions"]).includes(version),
          "Element of version list not in versions"
        );
      });
      af &= assertAndCheck(
        Object.keys(resource["versions"]).includes(resource["default-variant"]),
        "default variant not included in resource"
      );
      Object.keys(resource["versions"]).forEach((key) => {
        af &= assertAndCheck(
          resource["all-versions"].includes(key),
          "Version in version object not in version list"
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
