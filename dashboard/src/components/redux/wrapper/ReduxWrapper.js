import React from "react";
import { Provider } from "react-redux";

const ReduxWrapper = ({ children, store }) => (
  <Provider store={store}>{children}</Provider>
);

export default ReduxWrapper;
