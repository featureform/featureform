import React from "react";
import "jest-canvas-mock";
import ReduxWrapper from "components/redux/wrapper";
import { configure, mount } from "enzyme";
import Adapter from "@wojtekmaj/enzyme-adapter-react-17";
import { newTestStore } from "components/redux/store";
import { resourceTypes, testData } from "api/resources";
import ResourceList, { makeSelectFilteredResources } from "./ResourceList.js";

configure({ adapter: new Adapter() });

describe("ResourceList", () => {
  const dataType = resourceTypes["SPACE"];
  const mockFn = jest.fn(() => wrapInPromise(testData));
  const mockApi = {
    fetchResources: mockFn,
  };

  const component = mount(
    <ReduxWrapper store={newTestStore()}>
      <ResourceList api={mockApi} type={dataType} />
    </ReduxWrapper>
  );

  it("fetches resources on mount.", () => {
    expect(mockFn.mock.calls.length).toBe(1);
    expect(mockFn.mock.calls[0][0]).toEqual(dataType);
  });

  it("correctly maps inital props from state.", () => {
    const viewProps = component.find("ResourceListView").props();
    expect(viewProps).toMatchObject({
      activeVersions: {},
      title: dataType,
      resources: null,
      loading: true,
      failed: false,
    });
    const expKeys = [
      "activeTags",
      "activeVersions",
      "title",
      "resources",
      "loading",
      "failed",
      "setVersion",
      "toggleTag",
    ];
    expect(Object.keys(viewProps).sort()).toEqual(expKeys.sort());
  });

  describe("Resource Filter", () => {
    it("returns null when resources isn't set", () => {
      const state = {
        resourceList: { [dataType]: [] },
        selectedTags: { [dataType]: {} },
        selectedVersion: { [dataType]: {} },
      };
      const selector = makeSelectFilteredResources(dataType);
      expect(selector(state)).toBeNull();
    });

    it("doesn't filter when no tags are selected", () => {
      const resList = [{ name: "a", tags: ["1", "2"] }, { name: "b" }];
      const state = {
        resourceList: { [dataType]: { resources: resList } },
        selectedTags: { [dataType]: {} },
        selectedVersion: { [dataType]: {} },
      };
      const selector = makeSelectFilteredResources(dataType);
      expect(selector(state)).toEqual(resList);
    });

    it("filters using tag", () => {
      const resList = [
        { name: "a", versions: { a1: { tags: ["1", "2"] } } },
        { name: "b", versions: { b1: { tags: [] } } },
        { name: "c", versions: { c1: { tags: ["1"] } } },
        { name: "d", versions: { d1: { tags: ["2"] } } },
      ];
      const state = {
        resourceList: { [dataType]: { resources: resList } },
        selectedTags: { [dataType]: { "1": true } },
        selectedVersion: { [dataType]: { a: "a1", b: "b1", c: "c1", d: "d1" } },
      };
      const selector = makeSelectFilteredResources(dataType);
      const expected = [0, 2].map((idx) => resList[idx]);
      expect(selector(state)).toEqual(expected);
    });

    it("filters using multiple tags", () => {
      const resList = [
        { name: "a", versions: { a1: { tags: ["1", "2"] } } },
        { name: "b", versions: { b1: { tags: [] } } },
        { name: "c", versions: { c1: { tags: ["1"] } } },
        { name: "d", versions: { d1: { tags: ["2"] } } },
      ];
      const state = {
        resourceList: { [dataType]: { resources: resList } },
        selectedTags: { [dataType]: { "1": true, "2": true } },
        selectedVersion: { [dataType]: { a: "a1", b: "b1", c: "c1", d: "d1" } },
      };
      const selector = makeSelectFilteredResources(dataType);
      const expected = [0].map((idx) => resList[idx]);
      expect(selector(state)).toEqual(expected);
    });
  });
});
