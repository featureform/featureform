import React from "react";
// Necessary to get MaterialTable to work correctly.
import "jest-canvas-mock";
import { configure, shallow } from "enzyme";
import Adapter from "@wojtekmaj/enzyme-adapter-react-17";
import { BrowserRouter, MemoryRouter } from "react-router-dom";
import { createBrowserHistory } from "history";

import { resourceTypes } from "api/resources";
import {
  App,
  indexPath,
  parseContentProps,
  ThemeWrapper,
  views,
} from "./App.js";

configure({ adapter: new Adapter() });

describe("App", () => {
  const example_sections = [
    {
      name: "Resources",
      items: [
        {
          title: "Primary Data",
          icon: "file-import",
          path: "/primary-data",
          view: views.RESOURCE_LIST,
          viewProps: { type: resourceTypes.PRIMARY_DATA },
        },
        {
          title: "Materialized Views",
          icon: "copy",
          path: "/views",
          view: views.EMPTY,
        },
        {
          title: "Features",
          icon: "file-code",
          path: "/features",
          // view should be set to views.EMPTY by default.
        },
      ],
    },
    {
      name: "Admin",
      items: [
        { title: "Users", icon: "users", path: "/users", view: views.EMPTY },
        {
          title: "Documentation",
          icon: "book",
          path: "https://docs.streamsql.io",
          external: true,
        },
      ],
    },
  ];

  it("hasn't changed", () => {
    expect(
      shallow(
        <BrowserRouter>
          <App sections={example_sections} />
        </BrowserRouter>
      )
    ).toMatchSnapshot();
  });

  describe("Wrapper", () => {
    it("hasn't changed", () => {
      expect(
        shallow(
          <ThemeWrapper>
            <div>abc</div>
          </ThemeWrapper>
        )
      ).toMatchSnapshot();
    });
  });

  describe("parseContentProps", () => {
    it("parses correctly", () => {
      const expected = [
        {
          title: "Primary Data",
          path: "/primary-data",
          view: views.RESOURCE_LIST,
          viewProps: { type: resourceTypes.PRIMARY_DATA },
        },
        {
          title: "Materialized Views",
          path: "/views",
          view: views.EMPTY,
          viewProps: {},
        },
        {
          title: "Features",
          path: "/features",
          view: views.EMPTY,
          viewProps: {},
        },
        { title: "Users", path: "/users", view: views.EMPTY, viewProps: {} },
      ];
      const actual = parseContentProps(example_sections);
      expect(actual).toEqual(expected);
    });
  });

  describe("indexPath", () => {
    it("choses first path", () => {
      const content = parseContentProps(example_sections);
      expect(indexPath(content)).toEqual("/primary-data");
    });

    it("throws on empty content", () => {
      // When catching the error, the throwing call must be wrapped in a lambda:
      // See: https://jestjs.io/docs/en/expect.html#tothrowerror
      expect(() => indexPath([])).toThrow();
    });
  });
});
