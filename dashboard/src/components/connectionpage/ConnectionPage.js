import React, { useEffect } from "react";
import { fetchStatus } from "./ConnectionPageSlice";
import MetricsAPI from "api/resources/Metrics.js";
import ConnectionPageView from "./ConnectionPageView.js";
import ResourcesAPI from "api/resources";
import { connect } from "react-redux";

const mapDispatchToProps = (dispatch) => {
  return {
    fetchStatus: (api, resource) => dispatch(fetchStatus({ api, resource })),
  };
};

const metricsAPI = new MetricsAPI();
const resourcesAPI = new ResourcesAPI();

export const ConnectionPage = ({ connectionStatus, fetchStatus }) => {
  useEffect(() => {
    fetchStatus(metricsAPI, "metrics");
    fetchStatus(resourcesAPI, "resources");
  }, [fetchStatus]);
  return <ConnectionPageView statuses={connectionStatus.statuses} />;
};

function mapStateToProps(state) {
  return {
    connectionStatus: state.connectionStatus,
  };
}

export default connect(mapStateToProps, mapDispatchToProps)(ConnectionPage);
