// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { Box } from '@mui/material';
import { styled } from '@mui/system';
import { Chart, registerables } from 'chart.js';
import React, { createRef, useCallback, useEffect, useRef } from 'react';
import { connect } from 'react-redux';
import { PROMETHEUS_URL } from '../../../api/resources';

Chart.register(...registerables);

const GraphBox = styled(Box)({
  height: '50%',
});

const minutesToMilliseconds = (minutes) => {
  return parseInt(minutes * 60 * 1000);
};

const PrometheusGraph = ({
  query,
  time,
  timeRange,
  metricsSelect,
  type,
  name,
  query_type,
  add_labels,
  remote,
}) => {
  let max = 1000;
  if (query.includes('error')) {
    max = 10;
  } else if (query_type === 'latency' && type === 'TrainingSet') {
    max = 10;
  } else if (query_type === 'latency' && type === 'Feature') {
    max = 0.1;
  }

  const add_labels_string = add_labels
    ? Object.keys(add_labels).reduce(
        (acc, label) => `${acc} ${label}:"${add_labels[label]}"`,
        ''
      )
    : '';

  const customReq = useCallback(
    async (start, end, step) => {
      const startTimestamp = start.getTime() / 1000;
      const endTimestamp = end.getTime() / 1000;
      const url = `${PROMETHEUS_URL}/api/v1/query_range?query=${query}${add_labels_string}&start=${startTimestamp}&end=${endTimestamp}&step=${step}s`;
      return await fetch(url)
        .then((response) => response.json())
        .then((response) => {
          return response['data'];
        })
        .catch((err) => {
          //log the error and return an empty response
          console.error(err);
          return {
            resultType: 'matrix',
            result: [],
          };
        });
    },
    [query, add_labels_string, remote]
  );

  useEffect(() => {
    const chartRef = createRef();
    var myChart = new Chart(chartRef.current, {
      type: 'line',
      // plugins: [require('chartjs-plugin-datasource-prometheus')],
      options: {
        maintainAspectRatio: false,
        fillGaps: true,
        tension: 0,
        fill: true,
        animation: {
          duration: 0,
        },
        responsive: false,
        legend: {
          display: false,
        },
        tooltips: {
          enabled: false,
        },
        scales: {
          x: {
            type: 'time',
            ticks: {
              autoSkip: true,
              maxTicksLimit: 15,
            },
          },

          y: [
            {
              ticks: {
                autoSkip: true,
                maxTicksLimit: 8,
                beginAtZero: true,
              },
            },
          ],
        },
        plugins: {
          'datasource-prometheus': {
            query: customReq,
            timeRange: {
              type: 'relative',
              //timestamps in miliseconds relative to current time.
              //negative is the past, positive is the future
              start: -minutesToMilliseconds(timeRange.timeRange[0]),
              end: -minutesToMilliseconds(timeRange.timeRange[1]),
              msUpdateInterval: 5000,
            },
          },
        },
      },
    });

    return () => {
      setTimeout(() => {
        myChart?.destroy();
      }, 100);
    };
  }, [
    query,
    time,
    timeRange,
    metricsSelect.metrics,
    type,
    name,
    add_labels_string,
    customReq,
    max,
  ]);

  const chartRef = useRef(null);

  return (
    <GraphBox>
      <canvas
        height='300vw'
        style={{ maxHeight: '20em', width: '100%' }}
        ref={chartRef}
      />
    </GraphBox>
  );
};

function mapStateToProps(state) {
  return {
    timeRange: state.timeRange,
    metricsSelect: state.metricsSelect,
  };
}

export default connect(mapStateToProps)(PrometheusGraph);
