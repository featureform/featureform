import React from "react";
import { configure, mount } from "enzyme";
import Adapter from "@wojtekmaj/enzyme-adapter-react-17";
import { connect } from "react-redux";
import { configureStore, createSlice } from "@reduxjs/toolkit";

import ReduxWrapper from "../src/components/redux/wrapper/ReduxWrapper.js";

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

  const reduxComponent = mount(
    <ReduxWrapper store={testStore}>
      <ConnectedDiv />
    </ReduxWrapper>
  );

  it("Add store to context and test mapStateToProps", () => {
    const divProps = reduxComponent.find("StatefulDiv").props();
    expect(divProps).toMatchObject({
      valueProp: 5,
    });
  });
});
