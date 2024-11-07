// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { ThemeProvider } from '@mui/system';
import MaterialTable from 'material-table';
import React from 'react';
import theme from '../../styles/theme';

const FeatureSetList = ({ data }) => {
  const initRes = data || [];
  const copy = (res) => res.map((o) => ({ ...o }));
  // MaterialTable can't handle immutable object, we have to make a copy
  // https://github.com/mbrn/material-table/issues/666
  const mutableRes = copy(initRes);

  return (
    <ThemeProvider theme={theme}>
      <MaterialTable
        title='Feature Sets'
        columns={[
          { title: 'Name', field: 'name' },
          { title: 'Created', field: 'created' },
        ]}
        data={mutableRes}
        options={{
          search: true,
          draggable: false,
        }}
      />
    </ThemeProvider>
  );
};

export default FeatureSetList;
