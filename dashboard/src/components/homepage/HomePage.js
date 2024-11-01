// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import React from 'react';
import ResourcesAPI from '../../api/resources/Resources';
import DataPage from '../../components/datapage/DataPage';

const HomePage = () => {
  const apiHandle = new ResourcesAPI();
  return <DataPage api={apiHandle} type={'training-sets'} />;
};

export default HomePage;
