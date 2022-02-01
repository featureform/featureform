import React, { useEffect } from "react";
import { Typography, Grid, Container } from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import TimeDropdown from "./TimeDropdown";
import AggregateDropdown from "./AggregateDropdown";
import QueryDropdown from "./QueryDropdown";
import { connect } from "react-redux";

const useStyles = makeStyles((theme) => ({
  root: {
    flexGrow: 1,
    padding: theme.spacing(0),
    backgroundColor: theme.palette.background.paper,
    flexBasis: theme.spacing(0),
    flexDirection: "row",
    "& > *": {
      padding: theme.spacing(0),
    },
  },
  summaryData: {
    padding: theme.spacing(0),
  },
  titleBar: {
    display: "flex",
    justifyContent: "space-between",
  },
  aggDropdown: {
    minWidth: "20em",
    display: "flex",
    padding: theme.spacing(1),
  },
  graphTitle: {
    marginTop: "auto",
    marginBottom: "auto",
  },
  summaryItemDetail: {
    display: "flex",
    justifyContent: "space-between",
    padding: theme.spacing(1),
  },
  actionItemDetail: {
    display: "flex",
    justifyContent: "space-between",
    padding: theme.spacing(1),
  },
  summaryAddedDesc: {
    paddingLeft: theme.spacing(1),
  },
  timeSlider: {
    width: "20%",
    transform: "scale(0.9, 0.9)",
  },
  graph: {
    height: "70em",
    alignItems: "center",
    "& > *": {
      height: "70em",
    },
  },
}));

const MetricsDropdown = ({ type, name, version, timeRange, aggregates }) => {
  const classes = useStyles();
  const [stepRange, setStepRange] = React.useState("min");
  const [step, setStep] = React.useState("1m");
  useEffect(() => {
    if (timeRange.timeRange[0] > 60) {
      setStepRange("hour");
      setStep("1h");
    } else if (timeRange.timeRange[0] == 60) {
      setStepRange("min");
      setStep("1m");
    }
  }, [timeRange, aggregates]);

  return (
    <div className={classes.root}>
      <Grid container spacing={0}>
        <Grid item xs={12} height="10em">
          <div className={classes.graph}>
            <Container minHeight={"1300px"}>
              {type != "Dataset" ? (
                <div>
                  <div className={classes.titleBar}>
                    <div className={classes.graphTitle}>
                      <Typography variant="h6">
                        Throughput (req/{stepRange})
                      </Typography>
                    </div>

                    <div className={classes.aggDropdown}>
                      <TimeDropdown />
                      <AggregateDropdown graph={0} />
                    </div>
                  </div>

                  <QueryDropdown
                    query={`rate(test_counter{feature="${name} ${version}",status="success"}[${step}])`}
                    type={type}
                    name={name}
                    query_type={"latency"}
                    aggregate={aggregates[0]}
                  />
                  <div className={classes.titleBar}>
                    <div className={classes.graphTitle}>
                      <Typography variant="h6">Average Latency (ms)</Typography>
                    </div>

                    <div className={classes.aggDropdown}>
                      <TimeDropdown />
                      <AggregateDropdown graph={1} />
                    </div>
                  </div>

                  <QueryDropdown
                    query={`rate(test_duration_seconds_${aggregates[0]}{feature="${name} ${version}"}[${step}])/rate(test_duration_seconds_count{feature="${name} ${version}"}[${step}])`}
                    type={type}
                    name={name}
                    query_type={"count"}
                    aggregate={aggregates[0]}
                  />
                </div>
              ) : (
                <div>
                  <div className={classes.titleBar}>
                    <div className={classes.graphTitle}>
                      <Typography variant="h6">
                        Throughput (rows/{stepRange})
                      </Typography>
                    </div>

                    <div className={classes.aggDropdown}>
                      <TimeDropdown />
                      <AggregateDropdown graph={1} />
                    </div>
                  </div>

                  <QueryDropdown
                    query={`rate(test_counter{feature="${name} ${version}",status="row serving"}[${step}])`}
                    type={type}
                    name={name}
                    query_type={"latency"}
                    aggregate={aggregates[1]}
                  />
                </div>
              )}

              <div className={classes.titleBar}>
                <div className={classes.graphTitle}>
                  <Typography variant="h6">Errors per {stepRange}</Typography>
                </div>

                <div className={classes.aggDropdown}>
                  <TimeDropdown />
                  <AggregateDropdown graph={2} />
                </div>
              </div>

              <QueryDropdown
                query={`rate(test_counter{feature="${name} ${version}",status="error"}[${step}])`}
                type={type}
                name={name}
                query_type={"count"}
                aggregate={aggregates[2]}
              />
            </Container>
          </div>
        </Grid>
      </Grid>
    </div>
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
