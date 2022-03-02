import React from "react";
import { connect } from "react-redux";
import ResourcesAPI from "../../api/resources";
import ResourceList from "../resource-list/ResourceList";
import { useParams } from "react-router-dom";
import NotFoundPage from "../notfoundpage/NotFoundPage";
import Resource from "api/resources/Resource.js";
const apiHandle = new ResourcesAPI();

const DataPage = ({ ...props }) => {
  const { paramType } = useParams();
  let type = Resource.pathToType[paramType];

  return (
    <div>
      {type ? <ResourceList api={apiHandle} type={type} /> : <NotFoundPage />}
    </div>
  );
};

export default DataPage;
