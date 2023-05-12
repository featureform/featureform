import React from 'react';
import ResourcesAPI from '../src/api/resources/Resources';
import SearchResults from '../src/components/searchresults/SearchResults';

const QueryWrapper = () => {
  const apiHandle = new ResourcesAPI();
  return <SearchResults api={apiHandle} />;
};

export default QueryWrapper;
