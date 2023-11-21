import React from 'react';
import ResourcesAPI from '../../api/resources';
import Resource from '../../api/resources/Resource.js';
import { LoadingDots } from '../../components/entitypage/EntityPage';
import NotFound from '../notfoundpage/NotFound';
import ResourceList from '../resource-list/ResourceList';
const apiHandle = new ResourcesAPI();

const DataPage = ({ type }) => {
  let resourceType = Resource.pathToType[type];
  let body = <></>;
  if (type === undefined && resourceType === undefined) {
    body = <LoadingDots />;
  } else if (resourceType) {
    body = <ResourceList api={apiHandle} type={resourceType} />;
  } else {
    body = <NotFound />;
  }
  return <>{body}</>;
};

export default DataPage;
