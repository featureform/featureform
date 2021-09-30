import React from "react";
import { configure, mount } from "enzyme";
import Adapter from "enzyme-adapter-react-16";
import { connect } from "react-redux";
import { configureStore, createSlice } from "@reduxjs/toolkit";

import ReduxWrapper from "./ReduxWrapper.js";

configure({ adapter: new Adapter() });

describe("ReduxWrapper", () => {
  const exampleSlice = createSlice({
    name: "example",
    initialState: {
      value: 5,
    },
  });

  const testStore = configureStore({
    reducer: exampleSlice.reducer,
  });

  const StatefulDiv = ({ valueProp }) => <div>{valueProp}</div>;

  const mapStateToProps = (state) => ({
    valueProp: state.value,
  });

  const ConnectedDiv = connect(mapStateToProps)(StatefulDiv);

  it("Add store to context to allow connect to work", () => {
    expect(
      mount(
        <ReduxWrapper store={testStore}>
          <ConnectedDiv />
        </ReduxWrapper>
      )
    ).toMatchSnapshot();
  });
});
