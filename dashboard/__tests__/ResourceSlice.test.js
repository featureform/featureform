import "jest-canvas-mock";
import deferred from "deferred";
import { configureStore } from "@reduxjs/toolkit";
import { newTestStore } from "../src/components/redux/store";
import {
  initialState,
  fetchResources,
  default as resourceReducer,
} from "../src/components/resource-list/ResourceSlice";
import { testData } from "../src/api/resources";

const dataType = "Feature";

describe("fetchResourcesThunk", () => {
  const wrapInPromise = (arr) => Promise.resolve({ data: arr });

  it("fetches resources with dispatch", async () => {
    const reduxStore = newTestStore();
    const mockApi = {
      fetchResources: jest.fn(() => wrapInPromise(testData)),
    };
    const data = await reduxStore.dispatch(
      fetchResources({ api: mockApi, type: dataType, strict: false })
    );
    expect(data.payload).toEqual(testData);
  });

  it("sets resources state with dispatch", async () => {
    const reduxStore = newTestStore();
    const mockApi = {
      fetchResources: jest.fn(() => wrapInPromise(testData)),
    };
    // fulfilled (or rejected) will be called after this returns. Not waiting
    // for this results in a race condition.
    await reduxStore.dispatch(fetchResources({ api: mockApi, type: dataType }));
    const state = reduxStore.getState();
    const resources = state.resourceList[dataType].resources;
    expect(resources).toEqual(testData);
  });

  it("doesn't run a new request when it has data", async () => {
    const reduxStore = newTestStore();
    const mockFetchResources = jest.fn();
    mockFetchResources
      .mockReturnValueOnce(wrapInPromise(testData))
      .mockReturnValueOnce(wrapInPromise(["abc"]));
    const mockApi = {
      fetchResources: mockFetchResources,
    };
    await reduxStore.dispatch(fetchResources({ api: mockApi, type: dataType }));
    // This one should be a no-op due to the asyncThunk condition.
    await reduxStore.dispatch(fetchResources({ api: mockApi, type: dataType }));
    const state = reduxStore.getState();
    const resources = state.resourceList[dataType].resources;
    expect(resources).toEqual(testData);
  });

  it("doesn't run a new request when loading", async () => {
    const reduxStore = newTestStore();
    const defer = deferred();
    const mockFetchResources = jest.fn();
    mockFetchResources
      .mockReturnValueOnce(defer.promise)
      .mockReturnValueOnce(wrapInPromise(["abc"]));
    const mockApi = {
      fetchResources: mockFetchResources,
    };
    // Don't await here since it'll wait for us to resolve the promise, hence
    // deadlock.
    const origDispatch = reduxStore.dispatch(
      fetchResources({ api: mockApi, type: dataType })
    );
    // The second promise is resolved, so we can await it.
    await reduxStore.dispatch(fetchResources({ api: mockApi, type: dataType }));
    defer.resolve({ data: testData });
    origDispatch.then(() => {
      const state = reduxStore.getState();
      const resources = state.resourceList[dataType].resources;
      expect(resources).toEqual(testData);
    });
  });
});

describe("ResourceReducers", () => {
  it("sets state to loading on pending", () => {
    const action = fetchResources.pending("requestID", { type: dataType });
    const state = resourceReducer({ [dataType]: {} }, action);
    expect(state[dataType].loading).toEqual(true);
  });

  it("unsets data on pending", () => {
    const action = fetchResources.pending("requestId", { type: dataType });
    const state = resourceReducer({ [dataType]: { data: [] } }, action);
    expect(state[dataType].resources).toEqual(null);
  });

  it("sets data on success", () => {
    const payload = testData;
    const requestId = "123";
    const action = fetchResources.fulfilled(payload, requestId, {
      type: dataType,
    });
    const state = resourceReducer(
      { [dataType]: { requestId: requestId } },
      action
    );
    expect(state[dataType].resources).toEqual(payload);
  });

  it("sets failed on rejected", () => {
    const requestId = "123";
    const action = fetchResources.rejected(null, requestId, { type: dataType });
    const state = resourceReducer(
      { [dataType]: { requestId: requestId } },
      action
    );
    expect(state[dataType].failed).toEqual(true);
  });

  it("clears failed on pending", () => {
    const payload = testData;
    const requestId = "123";
    const action = fetchResources.pending(requestId, { type: dataType });
    const state = resourceReducer(
      { [dataType]: { requestId: requestId, failed: true } },
      action
    );
    expect(state[dataType].failed).toEqual(false);
  });

  it("ignore old request on fulfilled", () => {
    const payload = testData;
    const oldRequestId = "456";
    const newRequestId = "123";
    const action = fetchResources.fulfilled(payload, oldRequestId, {
      type: dataType,
    });
    const state = resourceReducer(
      { [dataType]: { loading: true, requestId: newRequestId } },
      action
    );
    expect(state[dataType].loading).toEqual(true);
  });

  it("ignore old request on rejected", () => {
    const payload = testData;
    const oldRequestId = "456";
    const newRequestId = "123";
    const action = fetchResources.rejected(payload, oldRequestId, {
      type: dataType,
    });
    const state = resourceReducer(
      { [dataType]: { loading: true, requestId: newRequestId } },
      action
    );
    expect(state[dataType].loading).toEqual(true);
  });
});
