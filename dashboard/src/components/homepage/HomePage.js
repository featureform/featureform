import React from 'react';
import ResourcesAPI from '../../api/resources/Resources';
import DataPage from '../../components/datapage/DataPage';

const HomePage = () => {
  const apiHandle = new ResourcesAPI();
  return <DataPage api={apiHandle} type={'training-sets'} />;
};

export default HomePage;
