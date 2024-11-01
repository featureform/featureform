// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import ReactEighteenAdapter from '@cfaester/enzyme-adapter-react-18';
import { configureStore, createSlice } from '@reduxjs/toolkit';
import { configure, mount } from 'enzyme';
import React from 'react';
import { connect } from 'react-redux';
import ReduxWrapper from '../src/components/redux/wrapper/ReduxWrapper.js';

configure({ adapter: new ReactEighteenAdapter() });

describe('ReduxWrapper', () => {
  const exampleSlice = createSlice({
    name: 'example',
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

  it('Add store to context and test mapStateToProps', () => {
    const divProps = reduxComponent.find('StatefulDiv').props();
    expect(divProps).toMatchObject({
      valueProp: 5,
    });
  });
});
