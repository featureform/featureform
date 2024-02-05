import { useRouter } from 'next/router';
import React from 'react';
import ResourcesAPI from '../../src/api/resources/Resources';
import DataPage from '../../src/components/datapage/DataPage';
import JobPage from '../../src/components/jobs';

const DataPageRoute = () => {
  const router = useRouter();
  const { type } = router.query;
  const apiHandle = new ResourcesAPI();

  // todox: need better routing. separate static pages from the dynamic pages?
  let body;
  if (type === 'jobs') {
    body = <JobPage />;
  } else {
    body = <DataPage api={apiHandle} type={type} />;
  }

  return body;
};

export default DataPageRoute;
