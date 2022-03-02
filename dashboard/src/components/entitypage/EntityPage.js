import React, { useEffect } from "react";
import { useParams } from "react-router-dom";
import { connect } from "react-redux";
import { fetchEntity } from "./EntityPageSlice.js";
import EntityPageView from "./EntityPageView.js";
import Loader from "react-loader-spinner";
import Container from "@material-ui/core/Container";
import Paper from "@material-ui/core/Paper";
import { setVersion } from "../resource-list/VersionSlice.js";
import NotFoundPage from "../notfoundpage/NotFoundPage";
import Resource from "api/resources/Resource.js";

const mapDispatchToProps = (dispatch) => {
  return {
    fetch: (api, type, title) => dispatch(fetchEntity({ api, type, title })),
    setVersion: (type, name, version) =>
      dispatch(setVersion({ type, name, version })),
  };
};

function mapStateToProps(state) {
  return {
    entityPage: state.entityPage,
    activeVersions: state.selectedVersion,
  };
}

const LoadingDots = () => {
  return (
    <Container maxWidth="xl">
      <Paper elevation={3}>
        <Container maxWidth="sm">
          <Loader type="ThreeDots" color="grey" height={40} width={40} />
        </Container>
      </Paper>
    </Container>
  );
};

const checkIfEmpty = (object) => {
  return Object.keys(object).length === 0 && object.constructor === Object;
};

const EntityPage = ({ api, entityPage, activeVersions, ...props }) => {
  const { type, entity } = useParams();

  let resourceType = Resource[Resource.pathToType[type]];

  const fetchEntity = props.fetch;

  useEffect(() => {
    fetchEntity(api, type, entity);
  }, [type, entity, api, fetchEntity]);

  return (
    <div>
      <div>
        {entityPage.failed ? (
          <NotFoundPage />
        ) : checkIfEmpty(entityPage) || entityPage.loading ? (
          <LoadingDots />
        ) : (
          <EntityPageView
            entity={entityPage}
            setVersion={props.setVersion}
            activeVersions={activeVersions}
            typePath={type}
            resourceType={resourceType}
          />
        )}
      </div>
    </div>
  );
};

export default connect(mapStateToProps, mapDispatchToProps)(EntityPage);
