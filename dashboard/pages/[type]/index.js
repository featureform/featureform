import { useRouter } from 'next/router';
import React from 'react';
import ResourcesAPI from '../../src/api/resources/Resources';
import DataPage from '../../src/components/datapage/DataPage';

const DataPageRoute = () => {
  const router = useRouter();
  const { type } = router.query;
  const apiHandle = new ResourcesAPI();

  return <DataPage api={apiHandle} type={type} />;
};

export default DataPageRoute;
