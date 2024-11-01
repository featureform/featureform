// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { useRouter } from 'next/router';
import React, { useEffect, useState } from 'react';
import { connect } from 'react-redux';
import { LoadingDots } from '../../components/entitypage/EntityPage.js';
import { setVariant } from '../resource-list/VariantSlice.js';
import { fetchSearch } from './SearchResultsSlice';
import SearchResultsView from './SearchResultsView';

function useQuery() {
  const router = useRouter();
  return new URLSearchParams(router.query);
}

const SearchResults = ({ searchResults, api, setVariant, ...props }) => {
  const searchQuery = useQuery().get('q');
  const [loading, setLoading] = useState(false);
  const fetchQuery = props.fetch;

  useEffect(() => {
    const getQuery = async () => {
      if (api && searchQuery) {
        setLoading(true);
        await fetchQuery(api, searchQuery);
        setLoading(false);
      }
    };
    getQuery();
  }, [searchQuery, api, fetchQuery]);

  return (
    <>
      {loading ? (
        <LoadingDots />
      ) : (
        <SearchResultsView
          results={searchResults?.resources}
          searchQuery={searchQuery}
          setVariant={setVariant}
        />
      )}
    </>
  );
};

const mapStateToProps = (state) => ({
  searchResults: state.searchResults,
});

const mapDispatchToProps = (dispatch) => {
  return {
    fetch: (api, query) => dispatch(fetchSearch({ api, query })),
    setVariant: (type, name, variant) => {
      dispatch(setVariant({ type, name, variant }));
    },
  };
};

export default connect(mapStateToProps, mapDispatchToProps)(SearchResults);
