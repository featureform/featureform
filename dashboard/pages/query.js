// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import React from 'react';
import ResourcesAPI from '../src/api/resources/Resources';
import SearchResults from '../src/components/searchresults/SearchResults';

const QueryWrapper = () => {
  const apiHandle = new ResourcesAPI();
  return <SearchResults api={apiHandle} />;
};

export default QueryWrapper;
