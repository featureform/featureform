// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import React from 'react';
import { SearchTable } from './SearchTable.js';

const SearchResultsView = ({ results, searchQuery, setVariant }) => {
  return (
    <SearchTable
      rows={results}
      searchQuery={searchQuery}
      setVariant={setVariant}
    />
  );
};

export default SearchResultsView;
