import React from "react";
import { Typography, Grid, Container } from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import ExponentialTimeSlider from "./ExponentialTimeSlider";
import MetricsSelect from "./MetricsSelect";
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
    height: "20em",
    alignItems: "center",
    "& > *": {
      height: "20em",
    },
  },
}));

const MetricsDropdown = ({ type, name }) => {
  const classes = useStyles();

  return (
    <div className={classes.root}>
      <Grid container spacing={0}>
        <Grid item xs={3}>
          <Container>
            <MetricsSelect />
          </Container>
        </Grid>
        <Grid item xs={9} height="15em">
          <div className={classes.graph}>
            <Container minHeight={"800px"}>
              <QueryDropdown type={type} name={name} />
            </Container>
          </div>
          <div className={classes.timeSlider}>
            <Container>
              <Typography variant="body2">Time range</Typography>
              <ExponentialTimeSlider />
            </Container>
          </div>
        </Grid>
      </Grid>
    </div>
  );
};

export default MetricsDropdown;
