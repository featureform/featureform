import React from "react";
import { useEffect } from "react";
import { Chart } from "chart.js";
import { makeStyles } from "@material-ui/core/styles";

import { connect } from "react-redux";

const useStyles = makeStyles((theme) => ({
  graphBox: {
    height: "30%",
  },
}));

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
}) => {
  const classes = useStyles();
  const add_labels_string = add_labels
    ? Object.keys(add_labels).reduce(
        (acc, label) => `${acc} ${label}:"${add_labels[label]}"`,
        ""
      )
    : "";
  useEffect(() => {
    var myChart = new Chart(chartRef.current, {
      type: "line",

      plugins: [require("chartjs-plugin-datasource-prometheus")],
      options: {
        maintainAspectRatio: false,
        responsive: true,
        legend: {
          display: false,
        },
        tooltips: {
          enabled: false,
        },
        scales: {
          xAxes: [
            {
              type: "time",
              ticks: {
                autoSkip: true,
                maxTicksLimit: 15,
              },
            },
          ],
          yAxes: [
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
          "datasource-prometheus": {
            prometheus: {
              endpoint: "http://localhost:9090",
            },
            query: `${query}${add_labels_string}`,

            timeRange: {
              type: "relative",
              //timestamps in miliseconds relative to current time.
              //negative is the past, positive is the future
              start: -minutesToMilliseconds(timeRange.timeRange[0]),
              end: -minutesToMilliseconds(timeRange.timeRange[1]),
            },
          },
        },
      },
    });

    return () => {
      myChart.destroy();
    };
  }, [
    query,
    time,
    timeRange,
    metricsSelect.metrics,
    type,
    name,
    add_labels_string,
  ]);
  const chartRef = React.useRef(null);

  return (
    <div className={classes.graphBox}>
      <canvas
        style={{ height: "30%", maxHeight: "10em", width: "100%" }}
        ref={chartRef}
      />
    </div>
  );
};

function mapStateToProps(state) {
  return {
    timeRange: state.timeRange,
    metricsSelect: state.metricsSelect,
  };
}

export default connect(mapStateToProps)(PrometheusGraph);
