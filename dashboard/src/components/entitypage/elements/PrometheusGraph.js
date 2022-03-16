import React from "react";
import { useEffect, useCallback } from "react";
import { Chart } from "chart.js";
import { makeStyles } from "@material-ui/core/styles";

import { connect } from "react-redux";

const useStyles = makeStyles((theme) => ({
  graphBox: {
    height: "50%",
  },
}));

const minutesToMilliseconds = (minutes) => {
  return parseInt(minutes * 60 * 1000);
};

const sample_query_data = `{
  "resultType" : "matrix",
  "result" : [

     {
        "metric" : {
           "__name__" : "up",
           "job" : "node",
           "instance" : "localhost:9091"
        },
        "values" : [
          [ 1435781385.781, "0" ],
          [ 1435781400.781, "0" ],
          [ 1435781415.781, "0" ],
           [ 1435781430.781, "0" ],
           [ 1435781445.781, "0" ],
           [ 1435781460.781, "0" ]
        ]
     }
  ]
}`;

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
  if (query.includes("error")) {
    max = 10;
  } else if (query_type === "latency" && type === "Training Set") {
    max = 10;
  } else if (query_type === "latency" && type === "Feature") {
    max = 0.1;
  }
  const classes = useStyles();

  const add_labels_string = add_labels
    ? Object.keys(add_labels).reduce(
        (acc, label) => `${acc} ${label}:"${add_labels[label]}"`,
        ""
      )
    : "";

  const customReq = useCallback(
    (start, end, step, stub) => {
      const startTimestamp = start.getTime() / 1000;
      const endTimestamp = end.getTime() / 1000;
      step = 5;
      const url = `http://localhost:9090/api/v1/query_range?query=${query}${add_labels_string}&start=${startTimestamp}&end=${endTimestamp}&step=${step}s`;
      if (!remote) {
        return Promise.resolve(JSON.parse(sample_query_data));
      }
      return fetch(url)
        .then((response) => response.json())
        .then((response) => response["data"])
        .catch((error) => {
          console.log("Prometheus not running", error);
        });
    },
    [query, add_labels_string, remote]
  );

  useEffect(() => {
    var myChart = new Chart(chartRef.current, {
      type: "line",

      plugins: [require("chartjs-plugin-datasource-prometheus")],
      options: {
        maintainAspectRatio: false,
        fillGaps: true,
        tension: 0,
        fill: true,
        animation: {
          duration: 0,
        },
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
                max: max,
              },
            },
          ],
        },

        plugins: {
          "datasource-prometheus": {
            query: customReq,

            timeRange: {
              type: "relative",
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
    customReq,
    max,
  ]);
  const chartRef = React.useRef(null);

  return (
    <div className={classes.graphBox}>
      <canvas
        height="300vw"
        style={{ maxHeight: "20em", width: "100%" }}
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
