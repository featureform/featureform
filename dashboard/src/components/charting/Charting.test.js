import React from "react";
import { configure, shallow } from "enzyme";
import Adapter from "enzyme-adapter-react-16";
import renderer from "react-test-renderer";
import { createPortal } from "react-dom";
import ReactDOM from "react-dom";

configure({ adapter: new Adapter() });
//dummy tests to make sure testing integration works
describe("Addition", () => {
  it("knows that 2 and 2 make 4", () => {
    expect(2 + 2).toBe(4);
  });
});
