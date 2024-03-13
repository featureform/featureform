import { Box, Chip, Grid, Typography } from '@mui/material';
import { makeStyles } from '@mui/styles';
import React, { useEffect } from 'react';
import { connect } from 'react-redux';
import QueryDropdown from './QueryDropdown';
import TimeDropdown from './TimeDropdown';

const useStyles = makeStyles((theme) => ({
  root: {
    flexGrow: 1,
    padding: theme.spacing(0),
    backgroundColor: theme.palette.background.paper,
    flexBasis: theme.spacing(0),
    flexDirection: 'row',
    '& > *': {
      padding: theme.spacing(0),
    },
  },
  linkPromChip: {
    paddingLeft: theme.spacing(2),
  },
  summaryData: {
    padding: theme.spacing(0),
  },
  titleBar: {
    display: 'flex',
    justifyContent: 'space-between',
    '& > *': {
      paddingBottom: '0em',
    },
  },
  aggDropdown: {
    minWidth: '0em',
    display: 'flex',
    '& > *': {
      paddingBottom: '0em',
    },
  },
  graphTitle: {
    marginTop: 'auto',
    marginBottom: 'auto',
  },
  summaryItemDetail: {
    display: 'flex',
    justifyContent: 'space-between',
    padding: theme.spacing(1),
  },
  actionItemDetail: {
    display: 'flex',
    justifyContent: 'space-between',
    padding: theme.spacing(1),
  },
  summaryAddedDesc: {
    paddingLeft: theme.spacing(1),
  },
  timeSlider: {
    width: '20%',
    transform: 'scale(0.9, 0.9)',
  },
  graph: {
    height: '80em',
    alignItems: 'center',
    '& > *': {
      height: '70em',
    },
  },
}));

const queryFormats = {
  Feature: {
    Throughput: {
      query: function (name, variant, step) {
        return `rate(test_counter{feature="${name}",status="success",key="${variant}"}[${step}])`;
      },
      type: 'count',
    },
    Latency: {
      query: function (name, variant, step) {
        return `rate(test_duration_seconds_sum{feature="${name}",status="",key="${variant}"}[${step}])/rate(test_duration_seconds_count{feature="${name}",key="${variant}",status=""}[${step}])`;
      },
      type: 'latency',
    },
    Errors: {
      query: function (name, variant, step) {
        return `rate(test_counter{feature="${name}",key="${variant}",status="error"}[${step}])`;
      },
      type: 'count',
    },
  },
  TrainingSet: {
    Throughput: {
      query: function (name, variant, step) {
        return `rate(test_counter{feature="${name}",key="${variant}",status="training_row_serve"}[${step}])`;
      },
      type: 'count',
    },
    Errors: {
      query: function (name, variant, step) {
        return `rate(test_counter{feature="${name}",key="${variant}",status="error"}[${step}])`;
      },
      type: 'count',
    },
  },
};

const prometheusAPI = 'http://localhost:9090/api/v1/labels';

const MetricsDropdown = ({ type, name, variant, timeRange, aggregates }) => {
  const classes = useStyles();
  const [apiConnected, setAPIConnected] = React.useState(false);
  const [step, setStep] = React.useState('1m');

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
        <Box
          className={classes.root}
          style={{ height: totalChartHeight }}
          data-testid='viewPortId'
        >
          <Typography variant='body1' className={classes.linkPromChip}>
            Source:
          </Typography>
          <Chip
            variant='outlined'
            clickable={apiConnected}
            className={classes.linkChip}
            size='small'
            color={apiConnected ? 'secondary' : 'error'}
            onClick={linkToPrometheus}
            label={'Prometheus'}
          ></Chip>
          <Grid container spacing={0}>
            <Grid item xs={12} height='10em'>
              <div className={classes.graph}>
                <Box>
                  {Object.entries(queryFormats[type]).map(
                    ([query_name, query_data], i) => {
                      return (
                        <div key={i}>
                          <div className={classes.titleBar}>
                            <div className={classes.graphTitle}>
                              <Typography variant='h6'>{query_name}</Typography>
                            </div>
                            <div className={classes.aggDropdown}>
                              <TimeDropdown />
                              {/*<AggregateDropdown graph={i} />*/}
                            </div>
                          </div>
                          <QueryDropdown
                            query={query_data.query(name, variant, step)}
                            type={type}
                            name={name}
                            query_type={query_data.type}
                            aggregate={aggregates[i]}
                            remote={apiConnected}
                          />
                        </div>
                      );
                    }
                  )}
                </Box>
              </div>
            </Grid>
          </Grid>
        </Box>
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
