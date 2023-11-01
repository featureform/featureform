import React from 'react';
import ResourcesAPI from '../../api/resources';
import Resource from '../../api/resources/Resource.js';
import NotFound from '../notfoundpage/NotFound';
import ResourceList from '../resource-list/ResourceList';
const apiHandle = new ResourcesAPI();

const DataPage = ({ type }) => {
  let resourceType = Resource.pathToType[type];
  return (
    <div>
      {resourceType ? (
        <ResourceList api={apiHandle} type={resourceType} />
      ) : (
        <NotFound />
      )}
    </div>
  );
};

export default DataPage;
