// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { Box, Chip, Grid, Typography } from '@mui/material';
import { styled } from '@mui/system';
import React, { useEffect, useState } from 'react';
import { connect } from 'react-redux';
import QueryDropdown from './QueryDropdown';
import TimeDropdown from './TimeDropdown';

const Root = styled(Box)(({ theme }) => ({
  flexGrow: 1,
  padding: theme.spacing(0),
  backgroundColor: theme.palette.background.paper,
  flexBasis: theme.spacing(0),
  flexDirection: 'row',
  '& > *': {
    padding: theme.spacing(0),
  },
}));

const TitleBar = styled(Box)({
  display: 'flex',
  justifyContent: 'space-between',
  '& > *': {
    paddingBottom: '0em',
  },
});

const AggDropdown = styled(Box)({
  minWidth: '0em',
  display: 'flex',
  '& > *': {
    paddingBottom: '0em',
  },
});

const GraphTitle = styled(Box)({
  marginTop: 'auto',
  marginBottom: 'auto',
});

const Graph = styled(Box)({
  height: '80em',
  alignItems: 'center',
  '& > *': {
    height: '70em',
  },
});

const queryFormats = {
  Feature: {
    Throughput: {
      query: function (name, variant, step) {
        return `rate(requests{name="${name}",status="success",variant="${variant}"}[${step}])`;
      },
      type: 'count',
    },
    Latency: {
      query: function (name, variant, step) {
        return `rate(request_duration_sum{name="${name}",status="",variant="${variant}"}[${step}])/rate(request_duration_count{name="${name}",variant="${variant}",status=""}[${step}])`;
      },
      type: 'latency',
    },
    Errors: {
      query: function (name, variant, step) {
        return `rate(requests{name="${name}",variant="${variant}",status="error"}[${step}])`;
      },
      type: 'count',
    },
  },
  TrainingSet: {
    Throughput: {
      query: function (name, variant, step) {
        return `rate(requests{name="${name}",variant="${variant}",status="training_row_serve"}[${step}])`;
      },
      type: 'count',
    },
    Errors: {
      query: function (name, variant, step) {
        return `rate(requests{name="${name}",variant="${variant}",status="error"}[${step}])`;
      },
      type: 'count',
    },
  },
};

const prometheusAPI = 'http://localhost:9090/api/v1/labels';

const MetricsDropdown = ({ type, name, variant, timeRange, aggregates }) => {
  const [apiConnected, setAPIConnected] = useState(false);
  const [step, setStep] = useState('1m');

  const createPromUrl = () => {
    /*eslint-disable react/jsx-key*/
    let urlPrefix = 'http://localhost:9090/graph?';
    let urlParamPart = Object.entries(queryFormats[type])
      .map(([, query_data], i) => {
        return `g${i}.expr=${encodeURIComponent(
          query_data.query(name, variant, step)
        )} &g${i}.tab=0&g${i}.stacked=0&g${i}.show_exemplars=0&g${i}.range_input=1h`;
      })
      .join('&');
    let url = urlPrefix + urlParamPart;
    return url;
  };

  useEffect(() => {
    fetch(prometheusAPI)
      .then(() => setAPIConnected(true))
      .catch(() => setAPIConnected(false));
    if (timeRange.timeRange[0] > 60) {
      setStep('1h');
    } else if (timeRange.timeRange[0] === 60) {
      setStep('1m');
    }
  }, [timeRange, aggregates]);

  const linkToPrometheus = () => {
    if (apiConnected) {
      window.location.href = createPromUrl();
    }
  };
  let totalChartHeight = 0;
  const queryObj = queryFormats[type];
  if (queryObj) {
    const totalProps = Object.keys(queryObj)?.length;
    totalChartHeight = totalProps ? totalProps * 500 : 0;
  }
  return (
    <>
      {type in queryFormats ? (
        <Root style={{ height: totalChartHeight }} data-testid='viewPortId'>
          <Typography variant='body1' sx={{ paddingLeft: 2 }}>
            Source:
          </Typography>
          <Chip
            variant='outlined'
            clickable={apiConnected}
            size='small'
            color={apiConnected ? 'secondary' : 'error'}
            onClick={linkToPrometheus}
            label={'Prometheus'}
          />
          <Grid container spacing={0}>
            <Grid item xs={12} height='10em'>
              <Graph>
                <Box>
                  {Object.entries(queryFormats[type]).map(
                    ([query_name, query_data], i) => (
                      <div key={i}>
                        <TitleBar>
                          <GraphTitle>
                            <Typography variant='h6'>{query_name}</Typography>
                          </GraphTitle>
                          <AggDropdown>
                            <TimeDropdown />
                            {/*<AggregateDropdown graph={i} />*/}
                          </AggDropdown>
                        </TitleBar>
                        <QueryDropdown
                          query={query_data.query(name, variant, step)}
                          type={type}
                          name={name}
                          query_type={query_data.type}
                          aggregate={aggregates[i]}
                          remote={apiConnected}
                        />
                      </div>
                    )
                  )}
                </Box>
              </Graph>
            </Grid>
          </Grid>
        </Root>
      ) : null}
    </>
  );
};

function mapStateToProps(state) {
  return {
    timeRange: state.timeRange,
    metricsSelect: state.metricsSelect,
    aggregates: state.aggregates,
  };
}

export default connect(mapStateToProps)(MetricsDropdown);
