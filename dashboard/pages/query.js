import React from 'react';
import SearchResults from '../src/components/searchresults/SearchResults';
import ResourcesAPI from '../src/api/resources/Resources';

const QueryWrapper = () => {
  const apiHandle = new ResourcesAPI();
  return <SearchResults api={apiHandle} />;
};

export default QueryWrapper;
