import React from "react";
import { Typography, Grid, Container } from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import TimeDropdown from "./TimeDropdown";
import QueryDropdown from "./QueryDropdown";

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
    height: "40em",
    alignItems: "center",
    "& > *": {
      height: "40em",
    },
  },
}));

const exampleStats = {
  Feature: {
    "Average Latency": "42ms",
    "95 Latency": "57ms",
    "99 Latency": "99ms",
    "Error Rate": "Minimal",
  },
  "Feature Set": {
    "Average Latency": "65ms",
    "95 Latency": "78ms",
    "99 Latency": "105ms",
    "Error Rate": "Average",
  },
  Entity: {
    Features: 44,
    "Primary Provider": "Snowflake",
  },
};

const StatsDropdown = ({ type, name }) => {
  const classes = useStyles();

  const stats = exampleStats[type];

  return (
    <div className={classes.root}>
      <Grid container spacing={0}>
        <Grid item xs={12} height="15em">
          <div className={classes.graph}>
            <Container minHeight={"800px"}>
              {Object.keys(stats).map((key, i) => (
                <Typography variant="body1">
                  <b>{key}: </b>
                  {stats[key]}
                </Typography>
              ))}
            </Container>
          </div>
        </Grid>
      </Grid>
    </div>
  );
};

export default StatsDropdown;
