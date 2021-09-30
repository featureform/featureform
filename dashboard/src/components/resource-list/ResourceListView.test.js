import React from "react";
// Necessary to get MaterialTable to work correctly.
import "jest-canvas-mock";
import { configure, shallow, mount } from "enzyme";
import Adapter from "enzyme-adapter-react-16";
import Chip from "@material-ui/core/Chip";
import produce from "immer";

import {
  ResourceListView,
  TagList,
  VersionSelector,
} from "./ResourceListView.js";

configure({ adapter: new Adapter() });

describe("ResourceListView", () => {
  it("passes through title", () => {
    const list = shallow(<ResourceListView title="test" title={"test"} />);
    expect(list.children().props().title).toBe("test");
  });

  it("sets resources to [] by default", () => {
    const list = shallow(<ResourceListView title="test" />);
    expect(list.children().props().data).toEqual([]);
  });

  it("passes through resources", () => {
    const list = shallow(
      <ResourceListView
        title="test"
        resources={[
          {
            name: "abc",
            versions: { "first-version": {}, "second-version": {} },
          },
        ]}
      />
    );
    expect(list.children().props().data).toEqual([{ name: "abc" }]);
  });

  it("makes resources mutable", () => {
    const immutData = produce([], (draft) => {
      draft.push({
        name: "abc",
        versions: { "first-version": {}, "second-version": {} },
      });
    });
    const list = shallow(
      <ResourceListView title="test" resources={immutData} />
    );
    expect(Object.isFrozen(immutData)).toBe(true);
    expect(Object.isFrozen(list.children().props().data)).toBe(false);
  });

  it("sets isLoading when resources isn't set", () => {
    const list = shallow(<ResourceListView title="test" />);
    expect(list.children().props().isLoading).toEqual(true);
  });

  it("sets isLoading when loading", () => {
    const list = shallow(<ResourceListView title="test" loading={true} />);
    expect(list.children().props().isLoading).toEqual(true);
  });

  it("sets isLoading when failed", () => {
    const list = shallow(
      <ResourceListView title="test" loading={false} failed={true} />
    );
    expect(list.children().props().isLoading).toEqual(true);
  });

  describe("VersionSelector", () => {
    it("sets default active version", () => {
      const sel = shallow(<VersionSelector name="abc" versions={["1", "2"]} />);
      expect(sel.children().props().value).toBe("1");
    });

    it("onChange calls setVersion", () => {
      const onChange = jest.fn();
      const name = "abc";
      const clickVal = "2";
      const sel = shallow(
        <VersionSelector
          name={name}
          versions={["1", "2"]}
          setVersion={onChange}
        />
      );
      sel.children().prop("onChange")({ target: { value: clickVal } });
      expect(onChange).toHaveBeenCalledWith(name, clickVal);
    });
  });

  describe("TagList", () => {
    const exampleTags = ["a", "b", "c"];

    it("renders correctly with no tags", () => {
      const list = shallow(<TagList />);
      expect(list.children().length).toBe(0);
    });

    it("renders correctly with no active tags", () => {
      const list = shallow(
        <TagList tagClass="class-here" tags={exampleTags} />
      );
      expect(list).toMatchSnapshot();
    });

    it("highlights active tags", () => {
      const list = mount(
        <TagList activeTags={{ [exampleTags[1]]: true }} tags={exampleTags} />
      );
      const chips = list.find(Chip);
      expect(chips.at(0).prop("color")).toBe("default");
      expect(chips.at(1).prop("color")).toBe("secondary");
    });

    it("toggles tag on click", () => {
      const toggle = jest.fn();
      const list = mount(<TagList tags={exampleTags} toggleTag={toggle} />);
      list.find(Chip).at(0).simulate("click");
      expect(toggle).toHaveBeenCalledWith(exampleTags[0]);
    });
  });
});
