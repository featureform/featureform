import React from "react";
import "jest-canvas-mock";
import ReduxWrapper from "../pages/components/redux/wrapper";
import { configure, mount } from "enzyme";
import Adapter from "@wojtekmaj/enzyme-adapter-react-17";
import { newTestStore } from "../pages/components/redux/store";
import { testData } from "../pages/api/resources";
import ResourceList, { makeSelectFilteredResources } from "../pages/components/resource-list/ResourceList";

configure({ adapter: new Adapter() });

describe("ResourceList", () => {
  const dataType = "Feature";
  const mockFn = jest.fn(() => wrapInPromise(testData));
  const mockApi = {
    fetchResources: mockFn,
  };

  const component = mount(
    <ReduxWrapper store={newTestStore()}>
      <ResourceList api={mockApi} type={dataType} type={dataType} />
    </ReduxWrapper>
  );

  it("fetches resources on mount.", () => {
    expect(mockFn.mock.calls.length).toBe(1);
    expect(mockFn.mock.calls[0][0]).toEqual(dataType);
  });

  it("correctly maps inital props from state.", () => {
    const viewProps = component.find("ResourceListView").props();
    expect(viewProps).toMatchObject({
      activeVariants: {},
      title: dataType,
      resources: null,
      loading: true,
      failed: false,
    });
    const expKeys = [
      "activeTags",
      "activeVariants",
      "title",
      "resources",
      "loading",
      "type",
      "failed",
      "setVariant",
      "toggleTag",
    ];
    expect(Object.keys(viewProps).sort()).toEqual(expKeys.sort());
  });

  describe("Resource Filter", () => {
    it("returns null when resources isn't set", () => {
      const state = {
        resourceList: { [dataType]: [] },
        selectedTags: { [dataType]: {} },
        selectedVariant: { [dataType]: {} },
      };
      const selector = makeSelectFilteredResources(dataType);
      expect(selector(state)).toBeNull();
    });

    it("doesn't filter when no tags are selected", () => {
      const resList = [{ name: "a", tags: ["1", "2"] }, { name: "b" }];
      const state = {
        resourceList: { [dataType]: { resources: resList } },
        selectedTags: { [dataType]: {} },
        selectedVariant: { [dataType]: {} },
      };
      const selector = makeSelectFilteredResources(dataType);
      expect(selector(state)).toEqual(resList);
    });

    it("filters using tag", () => {
      const resList = [
        { name: "a", variants: { a1: { tags: ["1", "2"] } } },
        { name: "b", variants: { b1: { tags: [] } } },
        { name: "c", variants: { c1: { tags: ["1"] } } },
        { name: "d", variants: { d1: { tags: ["2"] } } },
      ];
      const state = {
        resourceList: { [dataType]: { resources: resList } },
        selectedTags: { [dataType]: { "1": true } },
        selectedVariant: { [dataType]: { a: "a1", b: "b1", c: "c1", d: "d1" } },
      };
      const selector = makeSelectFilteredResources(dataType);
      const expected = [0, 2].map((idx) => resList[idx]);
      expect(selector(state)).toEqual(expected);
    });

    it("filters using multiple tags", () => {
      const resList = [
        { name: "a", variants: { a1: { tags: ["1", "2"] } } },
        { name: "b", variants: { b1: { tags: [] } } },
        { name: "c", variants: { c1: { tags: ["1"] } } },
        { name: "d", variants: { d1: { tags: ["2"] } } },
      ];
      const state = {
        resourceList: { [dataType]: { resources: resList } },
        selectedTags: { [dataType]: { "1": true, "2": true } },
        selectedVariant: { [dataType]: { a: "a1", b: "b1", c: "c1", d: "d1" } },
      };
      const selector = makeSelectFilteredResources(dataType);
      const expected = [0].map((idx) => resList[idx]);
      expect(selector(state)).toEqual(expected);
    });
  });
});
