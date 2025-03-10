// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import deferred from 'deferred';
import 'jest-canvas-mock';
import { testData } from '../src/api/resources';
import { newTestStore } from '../src/components/redux/store';
import {
  default as resourceReducer,
  fetchResources,
} from '../src/components/resource-list/ResourceSlice';

const dataType = 'Feature';

describe('fetchResourcesThunk', () => {
  const wrapInPromise = (arr) => Promise.resolve({ data: arr });

  it.skip('fetches resources with dispatch', async () => {
    const reduxStore = newTestStore();
    const apiMock = {
      fetchResources: jest.fn(() => wrapInPromise(testData)),
    };
    const data = await reduxStore.dispatch(
      fetchResources({ api: apiMock, type: dataType, strict: false })
    );
    expect(data.payload).toEqual(testData);
  });

  it('sets resources state with dispatch', async () => {
    const reduxStore = newTestStore();
    const apiMock = {
      fetchResources: jest.fn(() => wrapInPromise(testData)),
    };
    // fulfilled (or rejected) will be called after this returns. Not waiting
    // for this results in a race condition.
    await reduxStore.dispatch(fetchResources({ api: apiMock, type: dataType }));
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
      .mockReturnValueOnce(wrapInPromise(['abc']));
    const apiMock = {
      fetchResources: mockFetchResources,
    };
    // Don't await here since it'll wait for us to resolve the promise, hence
    // deadlock.
    const origDispatch = reduxStore.dispatch(
      fetchResources({ api: apiMock, type: dataType })
    );
    // The second promise is resolved, so we can await it.
    await reduxStore.dispatch(fetchResources({ api: apiMock, type: dataType }));
    /*eslint-disable jest/valid-expect-in-promise*/
    defer.resolve({ data: testData });
    origDispatch.then(() => {
      const state = reduxStore.getState();
      const resources = state.resourceList[dataType].resources;
      expect(resources).toEqual(testData);
    });
  });
});

describe('ResourceReducers', () => {
  it('sets state to loading on pending', () => {
    const action = fetchResources.pending('requestID', { type: dataType });
    const state = resourceReducer({ [dataType]: {} }, action);
    expect(state[dataType].loading).toEqual(true);
  });

  it('unsets data on pending', () => {
    const action = fetchResources.pending('requestId', { type: dataType });
    const state = resourceReducer({ [dataType]: { data: [] } }, action);
    expect(state[dataType].resources).toEqual(null);
  });

  it.skip('sets data on success', () => {
    const requestId = '123';
    const action = fetchResources.fulfilled(testData, requestId, {
      type: dataType,
    });
    const state = resourceReducer(
      { [dataType]: { requestId: requestId } },
      action
    );
    expect(state[dataType].resources).toEqual(testData);
  });

  it('sets failed on rejected', () => {
    const requestId = '123';
    const action = fetchResources.rejected(null, requestId, { type: dataType });
    const state = resourceReducer(
      { [dataType]: { requestId: requestId } },
      action
    );
    expect(state[dataType].failed).toEqual(true);
  });

  it('clears failed on pending', () => {
    const requestId = '123';
    const action = fetchResources.pending(requestId, { type: dataType });
    const state = resourceReducer(
      { [dataType]: { requestId: requestId, failed: true } },
      action
    );
    expect(state[dataType].failed).toEqual(false);
  });

  it('ignore old request on fulfilled', () => {
    const oldRequestId = '456';
    const newRequestId = '123';
    const action = fetchResources.fulfilled(testData, oldRequestId, {
      type: dataType,
    });
    const state = resourceReducer(
      { [dataType]: { loading: true, requestId: newRequestId } },
      action
    );
    expect(state[dataType].loading).toEqual(true);
  });

  it('ignore old request on rejected', () => {
    const oldRequestId = '456';
    const newRequestId = '123';
    const action = fetchResources.rejected(testData, oldRequestId, {
      type: dataType,
    });
    const state = resourceReducer(
      { [dataType]: { loading: true, requestId: newRequestId } },
      action
    );
    expect(state[dataType].loading).toEqual(true);
  });
});
