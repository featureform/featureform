import React from "react";
import { Typography, Grid, Container } from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import { Bar } from "react-chartjs-2";
import faker from "faker";

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
  errorBar: {
    width: "50%",
  },
  summaryAddedDesc: {
    paddingLeft: theme.spacing(1),
  },
  timeSlider: {
    width: "20%",
    transform: "scale(0.9, 0.9)",
  },
  barGraph: {
    maxWidth: "40em",
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
    "Mean Error Rate": "0.1922%",
  },
  "Feature Set": {
    "Average Latency": "65ms",
    "95 Latency": "78ms",
    "99 Latency": "105ms",
    "Mean Error Rate": "0.1922%",
  },
  Entity: {
    Features: 44,
    "Primary Provider": "Snowflake",
  },
};

export const options = {
  plugins: {
    title: {
      display: true,
      text: "Query Error Summary",
    },
  },
  responsive: true,
  scales: {
    x: {
      stacked: true,
    },
    y: {
      stacked: true,
    },
  },
};

const labels = ["Last 1000 queries", "Last 7 days", "Overall"];

export const data = {
  labels,
  datasets: [
    {
      label: "Client Errors",
      data: labels.map(() => faker.datatype.number({ min: 0, max: 100 })),
      backgroundColor: "rgb(255, 99, 132)",
    },
    {
      label: "Server Errors",
      data: labels.map(() => faker.datatype.number({ min: 0, max: 100 })),
      backgroundColor: "rgb(75, 192, 192)",
    },
    {
      label: "Provider Errors",
      data: labels.map(() => faker.datatype.number({ min: 0, max: 100 })),
      backgroundColor: "rgb(53, 162, 235)",
    },
  ],
};

const StatsDropdown = ({ type, name }) => {
  const classes = useStyles();

  const stats = exampleStats[type];

  return (
    <div className={classes.root}>
      <Grid container spacing={0}>
        <Grid item xs={12} height="15em">
          <div className={classes.graph}>
            <Container minheight={"800px"}>
              {Object.keys(stats).map((key, i) => (
                <Typography variant="body1">
                  <b>{key}: </b>
                  {stats[key]}
                </Typography>
              ))}
              <Typography variant="body1">
                <b>Error Stats:</b>
              </Typography>
              <div className={classes.errorBar}>
                <Bar
                  options={options}
                  data={data}
                  className={classes.barGraph}
                />
              </div>
            </Container>
          </div>
        </Grid>
      </Grid>
    </div>
  );
};

export default StatsDropdown;
