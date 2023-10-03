import React from 'react';
import ResourcesAPI from '../../api/resources';
import Resource from '../../api/resources/Resource.js';
import NotFoundPage from '../notfoundpage/NotFoundPage';
import ResourceList from '../resource-list/ResourceList';
const apiHandle = new ResourcesAPI();

const DataPage = ({ type }) => {
  let resourceType = Resource.pathToType[type];
  return (
    <div>
      {resourceType ? (
        <ResourceList api={apiHandle} type={resourceType} />
      ) : (
        <NotFoundPage />
      )}
    </div>
  );
};

export default DataPage;
