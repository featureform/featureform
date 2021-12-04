import React from "react";
import { Typography, Grid, Container } from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import ExponentialTimeSlider from "./ExponentialTimeSlider";
import TimeDropdown from "./TimeDropdown";
import QueryDropdown from "./QueryDropdown";

const useStyles = makeStyles((theme) => ({
  root: {
    flexGrow: 1,
    padding: theme.spacing(0),
    backgroundColor: theme.palette.background.paper,
    flexBasis: theme.spacing(1),
    flexDirection: "row",
    "& > *": {
      padding: theme.spacing(1),
    },
  },
  summaryData: {
    padding: theme.spacing(0),
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
    width: "60%",
  },
  graph: {
    height: "30em",
    alignItems: "center",
    "& > *": {
      height: "30em",
    },
  },
}));

const MetricsDropdown = ({ type, name }) => {
  const classes = useStyles();

  return (
    <div className={classes.root}>
      <div className={classes.timeSlider}>
        <Container>
          <TimeDropdown />
        </Container>
      </div>
      <Grid container spacing={0}>
        <Grid item xs={12} height="15em">
          <div className={classes.graph}>
            <Container minHeight={"800px"}>
              <Typography>Throughput (req/min)</Typography>
              <QueryDropdown
                query={`rate(test_counter{feature="Non-free Sulfur Dioxide",status="success"}[1m])`}
                type={type}
                name={name}
                query_type={"latency"}
              />
              <Typography> Average Latency (ms)</Typography>
              <QueryDropdown
                query={`rate(test_duration_seconds_sum{feature="Non-free Sulfur Dioxide"}[1m])/rate(test_duration_seconds_count{feature="Non-free Sulfur Dioxide"}[1m])`}
                type={type}
                name={name}
                query_type={"count"}
              />
              <Typography>Errors per minute</Typography>
              <QueryDropdown
                query={`rate(test_counter{feature="Non-free Sulfur Dioxide",status="error"}[1m])`}
                type={type}
                name={name}
                query_type={"count"}
              />
            </Container>
          </div>
        </Grid>
      </Grid>
    </div>
  );
};

export default MetricsDropdown;
