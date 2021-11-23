import React from "react";
import { useEffect } from "react";
import { Chart } from "chart.js";
import { connect } from "react-redux";

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
}) => {
  const query_type = "latency";
  useEffect(() => {
    var myChart = new Chart(chartRef.current, {
      type: "line",

      plugins: [require("chartjs-plugin-datasource-prometheus")],
      options: {
        maintainAspectRatio: false,
        responsive: true,
        plugins: {
          "datasource-prometheus": {
            prometheus: {
              endpoint: "http://localhost:9090",
            },
            query: `http_request_${query_type}{type="${type}" name="${name}"}`,
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
  }, [query, time, timeRange, metricsSelect.metrics, type, name]);
  const chartRef = React.useRef(null);

  return (
    <div>
      <canvas ref={chartRef} />
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
