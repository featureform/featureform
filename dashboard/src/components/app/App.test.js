import React from "react";
// Necessary to get MaterialTable to work correctly.
import "jest-canvas-mock";
import { configure, shallow } from "enzyme";
import Adapter from "enzyme-adapter-react-16";
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
          title: "Data Sources",
          icon: "file-import",
          path: "/sources",
          view: views.RESOURCE_LIST,
          viewProps: { type: resourceTypes.DATA_SOURCE },
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
          title: "Data Sources",
          path: "/sources",
          view: views.RESOURCE_LIST,
          viewProps: { type: resourceTypes.DATA_SOURCE },
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
      expect(indexPath(content)).toEqual("/sources");
    });

    it("throws on empty content", () => {
      // When catching the error, the throwing call must be wrapped in a lambda:
      // See: https://jestjs.io/docs/en/expect.html#tothrowerror
      expect(() => indexPath([])).toThrow();
    });
  });
});
