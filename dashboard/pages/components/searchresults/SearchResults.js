import React, { useEffect } from "react";
import { connect } from "react-redux";
import SearchResultsView from "./SearchResultsView";
import { useRouter } from 'next/router'
import { fetchSearch } from "./SearchResultsSlice";
import { setVariant } from "../resource-list/VariantSlice.js";


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
  let search_query = useQuery().get("q");
  const fetchQuery = props.fetch;
  useEffect(() => {
    fetchQuery(api, search_query);
  }, [search_query, api, fetchQuery]);

  return (
    <div>
      {searchResults.resources ? (
        <SearchResultsView
          results={searchResults.resources}
          search_query={search_query}
          setVariant={setVariant}
        />
      ) : (
        <div></div>
      )}
    </div>
  );
};

const mapStateToProps = (state) => ({
  searchResults: state.searchResults,
});

export default connect(mapStateToProps, mapDispatchToProps)(SearchResults);
