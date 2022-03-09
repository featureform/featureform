import React from "react";
// Necessary to get MaterialTable to work correctly.
import "jest-canvas-mock";
import { configure, shallow, mount } from "enzyme";
import Adapter from "@wojtekmaj/enzyme-adapter-react-17";
import Chip from "@material-ui/core/Chip";
import produce from "immer";
import CssBaseline from "@material-ui/core/CssBaseline";
import TEST_THEME from "styles/theme";
import { ThemeProvider } from "@material-ui/core/styles";

import {
  ResourceListView,
  TagList,
  VariantSelector,
} from "./ResourceListView.js";

configure({ adapter: new Adapter() });

export const ThemeWrapper = ({ children }) => (
  <ThemeProvider theme={TEST_THEME}>
    <CssBaseline />
    {children}
  </ThemeProvider>
);

export function mountWithTheme(child) {
  return mount(child, {
    wrappingComponent: ({ children }) => (
      <ThemeProvider theme={TEST_THEME}>{children}</ThemeProvider>
    ),
  });
}

export function shallowWithTheme(child) {
  return shallow(child, {
    wrappingComponent: ({ children }) => (
      <ThemeWrapper>{children}</ThemeWrapper>
    ),
  });
}

describe("ResourceListView", () => {
  it("sets resources to [] by default", () => {
    const list = shallow(<ResourceListView title="test" type="Feature" />);
    expect(list.children().props().data).toEqual([]);
  });

  it("passes through resources", () => {
    const list = shallow(
      <ResourceListView
        title="test"
        type="Feature"
        resources={[
          {
            name: "abc",
            variants: { "first-variant": {}, "second-variant": {} },
          },
        ]}
      />
    );
    expect(list.children().props().data).toEqual([
      { name: "abc", revision: "Invalid Date" },
    ]);
  });

  it("makes resources mutable", () => {
    const immutData = produce([], (draft) => {
      draft.push({
        name: "abc",
        variants: { "first-variant": {}, "second-variant": {} },
      });
    });
    const list = shallow(
      <ResourceListView title="test" resources={immutData} type="Feature" />
    );
    expect(Object.isFrozen(immutData)).toBe(true);
    expect(Object.isFrozen(list.children().props().data)).toBe(false);
  });

  it("sets isLoading when resources isn't set", () => {
    const list = shallow(<ResourceListView title="test" type="Feature" />);
    expect(list.children().props().isLoading).toEqual(true);
  });

  it("sets isLoading when loading", () => {
    const list = shallow(
      <ResourceListView title="test" loading={true} type="Feature" />
    );
    expect(list.children().props().isLoading).toEqual(true);
  });

  it("sets isLoading when failed", () => {
    const list = shallow(
      <ResourceListView
        title="test"
        loading={false}
        failed={true}
        type="Feature"
      />
    );
    expect(list.children().props().isLoading).toEqual(true);
  });

  describe("VariantSelector", () => {
    it("sets default active variant", () => {
      const sel = shallow(<VariantSelector name="abc" variants={["1", "2"]} />);
      expect(sel.children().props().value).toBe("1");
    });

    it("onChange calls setVariant", () => {
      const onChange = jest.fn();
      const name = "abc";
      const clickVal = "2";
      const sel = shallow(
        <VariantSelector
          name={name}
          variants={["1", "2"]}
          setVariant={onChange}
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
