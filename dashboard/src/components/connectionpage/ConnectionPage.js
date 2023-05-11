import React, { useEffect } from 'react';
import { connect } from 'react-redux';
import ResourcesAPI from '../../api/resources';
import MetricsAPI from '../../api/resources/Metrics.js';
import { fetchStatus } from './ConnectionPageSlice';
import ConnectionPageView from './ConnectionPageView.js';

const mapDispatchToProps = (dispatch) => {
  return {
    fetchStatus: (api, resource) => dispatch(fetchStatus({ api, resource })),
  };
};

const metricsAPI = new MetricsAPI();
const resourcesAPI = new ResourcesAPI();

export const ConnectionPage = ({ connectionStatus, fetchStatus }) => {
  useEffect(() => {
    fetchStatus(metricsAPI, 'metrics');
    fetchStatus(resourcesAPI, 'resources');
  }, [fetchStatus]);
  return <ConnectionPageView statuses={connectionStatus.statuses} />;
};

function mapStateToProps(state) {
  return {
    connectionStatus: state.connectionStatus,
  };
}

export default connect(mapStateToProps, mapDispatchToProps)(ConnectionPage);
