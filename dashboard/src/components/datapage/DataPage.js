import React from "react";
import { connect } from "react-redux";
import ResourcesAPI from "../../api/resources";
import ResourceList from "../resource-list/ResourceList";
import { useParams } from "react-router-dom";
import NotFoundPage from "../notfoundpage/NotFoundPage";
import Resource from "api/resources/Resource.js";
const apiHandle = new ResourcesAPI();

const DataPage = ({ ...props }) => {
  const { type } = useParams();
  let resourceType = Resource.pathToType[type];
  //let typeProp = { type: resourceType };

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
