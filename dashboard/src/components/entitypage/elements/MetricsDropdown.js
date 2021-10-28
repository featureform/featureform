import React from "react";
import LatencyGraph from "./LatencyGraph";
import { Divider, Typography, Grid, Container } from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import ExponentialTimeSlider from "./ExponentialTimeSlider";

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
    height: "250px",
    alignItems: "center",
    "& > *": {
      height: "250px",
    },
  },
}));

const MetricsDropdown = () => {
  const classes = useStyles();

  const summaryData = [
    { name: "Total Versions", value: 3 },
    { name: "Revision Size", value: 189, unit: "Vec." },
    { name: "P50 Latency", value: 18.9, unit: "ms" },
    { name: "P99 Latency", value: 113.2, unit: "ms" },
  ];

  const actionsData = [
    { action: "Revision Pushed", time: "2020-04-24T13:07:44.000+0000" },
    { action: "Revision Pushed", time: "2020-04-24T13:07:44.000+0000" },
    { action: "Version Created", time: "2020-04-24T13:07:44.000+0000" },
    { action: "Version Created", time: "2020-04-24T13:07:44.000+0000" },
    { action: "Revision Pushed", time: "2020-04-24T13:07:44.000+0000" },
  ];
  const data = [
    {
      id: "P50",
      color: "hsl(203, 70%, 50%)",
      data: [
        { x: "2020-04-24T13:07:44.000+0000", y: 18.9 },
        { x: "2020-04-24T13:08:20.012+0000", y: 18.8 },
        { x: "2020-04-24T13:08:50.050+0000", y: 18.2 },
        { x: "2020-04-24T13:08:55.050+0000", y: 17.9 },
        { x: "2020-04-24T13:09:46.050+0000", y: 17.9 },
        { x: "2020-04-24T13:10:46.050+0000", y: 18.3 },
        { x: "2020-04-24T13:11:46.050+0000", y: 18.5 },
        { x: "2020-04-24T13:12:46.050+0000", y: 18.6 },
        { x: "2020-04-24T13:13:46.050+0000", y: 18.75 },
        { x: "2020-04-24T13:14:20.050+0000", y: 18.0 },
      ],
    },
    {
      id: "P99",
      color: "hsl(188, 70%, 50%)",
      data: [
        { x: "2020-04-24T13:07:44.000+0000", y: 120 },
        { x: "2020-04-24T13:08:20.012+0000", y: 113 },
        { x: "2020-04-24T13:08:50.050+0000", y: 112 },
        { x: "2020-04-24T13:08:55.050+0000", y: 60 },
        { x: "2020-04-24T13:09:46.050+0000", y: 65 },
        { x: "2020-04-24T13:10:46.050+0000", y: 68 },
        { x: "2020-04-24T13:11:46.050+0000", y: 90 },
        { x: "2020-04-24T13:12:46.050+0000", y: 111 },
        { x: "2020-04-24T13:13:46.050+0000", y: 70 },
        { x: "2020-04-24T13:14:20.050+0000", y: 75 },
      ],
    },
  ];
  return (
    <div className={classes.root}>
      <Grid container spacing={0}>
        <Grid item xs={3}>
          <Container>
            <div className={classes.summaryData}>
              <Typography variant="h6" className={classes.summaryTitle}>
                Summary
              </Typography>
              {summaryData.map((data) => (
                <SummaryItem data={data} />
              ))}
            </div>
          </Container>
        </Grid>
        <Grid item xs={7} height="250px">
          <div className={classes.timeSlider}>
            <Container>
              <Typography variant="body2">Time range</Typography>
              <ExponentialTimeSlider />
            </Container>
          </div>
          <div className={classes.graph}>
            <Container>
              <Typography variant="h5">Latency</Typography>
              <LatencyGraph data={data} />
            </Container>
          </div>
        </Grid>
        <Grid item xs={2}>
          <Container>
            <div className={classes.summaryData}>
              <Typography variant="h6" className={classes.summaryTitle}>
                Actions
              </Typography>
              {actionsData.map((data) => (
                <ActionItem data={data} />
              ))}
            </div>
          </Container>
        </Grid>
      </Grid>
    </div>
  );
};

const SummaryItem = ({ data }) => {
  const classes = useStyles();
  return (
    <div>
      <div className={classes.summaryItemDetail}>
        <Typography variant="body1">{data.name}</Typography>
        <Typography variant="body1">
          {data.value} {data.unit ? data.unit : ""}
        </Typography>
      </div>
      <div className={classes.summaryAddedDesc}>
        <Typography variant="body2">
          {data.detail ? data.detail : ""}
        </Typography>
      </div>
      <div>
        <Divider />
      </div>
    </div>
  );
};

const ActionItem = ({ data }) => {
  const classes = useStyles();

  const rtf1 = new Intl.RelativeTimeFormat("en", { style: "narrow" });

  return (
    <div className={classes.actionItemDetail}>
      <Typography variant="body2">{data.action}</Typography>
      <Typography variant="body2">{rtf1.format(3, "quarter")}</Typography>
      <Divider />
    </div>
  );
};

export default MetricsDropdown;
