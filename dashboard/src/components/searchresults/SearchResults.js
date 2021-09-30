import React, { useEffect } from "react";
import { connect } from "react-redux";
import { useLocation } from "react-router-dom";
import SearchResultsView from "./SearchResultsView";
import { fetchSearch } from "./SearchResultsSlice";

function useQuery() {
  return new URLSearchParams(useLocation().search);
}

const mapDispatchToProps = (dispatch) => {
  return {
    fetch: (api, query) => dispatch(fetchSearch({ api, query })),
  };
};

const SearchResults = ({ searchResults, api, ...props }) => {
  let search_query = useQuery().get("q");

  const fetchQuery = props.fetch;
  useEffect(() => {
    fetchQuery(api, search_query);
  }, [search_query, api, fetchQuery]);

  return (
    <div>
      <SearchResultsView results={searchResults} search_query={search_query} />
    </div>
  );
};

const mapStateToProps = (state) => ({
  searchResults: state.searchResults,
});

export default connect(mapStateToProps, mapDispatchToProps)(SearchResults);
