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

const mapDispatchToProps = (dispatch) => {
  return {
    fetch: (api, query) => dispatch(fetchSearch({ api, query })),
    setVariant: (type, name, variant) => {
      dispatch(setVariant({ type, name, variant }));
    },
  };
};

const SearchResults = ({ searchResults, api, setVariant, ...props }) => {
  let search_query = useQuery().get('q');
  const [loading, setLoading] = useState(false);
  const fetchQuery = props.fetch;
  useEffect(async () => {
    if (api && search_query) {
      setLoading(true);
      await fetchQuery(api, search_query);
      setLoading(false);
    }
  }, [search_query, api, fetchQuery]);

  return (
    <>
      {loading ? (
        <LoadingDots />
      ) : (
        <SearchResultsView
          results={searchResults?.resources}
          search_query={search_query}
          setVariant={setVariant}
        />
      )}
    </>
  );
};

const mapStateToProps = (state) => ({
  searchResults: state.searchResults,
});

export default connect(mapStateToProps, mapDispatchToProps)(SearchResults);
