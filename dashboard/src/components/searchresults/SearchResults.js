import { useRouter } from 'next/router';
import React, { useEffect } from 'react';
import { connect } from 'react-redux';
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
  const fetchQuery = props.fetch;
  useEffect(() => {
    if (api && search_query) {
      fetchQuery(api, search_query);
    }
  }, [search_query, api, fetchQuery]);

  return (
    <div>
      <SearchResultsView
        results={searchResults?.resources}
        search_query={search_query}
        setVariant={setVariant}
      />
    </div>
  );
};

const mapStateToProps = (state) => ({
  searchResults: state.searchResults,
});

export default connect(mapStateToProps, mapDispatchToProps)(SearchResults);
