import React from "react";
import ResourcesAPI from "../../api/resources";
import ResourceList from "../resource-list/ResourceList";
import NotFoundPage from "../notfoundpage/NotFoundPage";
import Resource from "../../api/resources/Resource.js";
const apiHandle = new ResourcesAPI();

const DataPage = ({ type, ...props }) => {
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
