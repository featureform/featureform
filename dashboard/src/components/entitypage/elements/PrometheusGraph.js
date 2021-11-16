import React from "react";
import { useEffect } from "react";
import { Chart } from "chart.js";
import { connect } from "react-redux";

const PrometheusGraph = ({ query, time, timeRange }) => {
  const millisecondsSinceRangeStart =
    -parseInt(timeRange.timeRange[0]) * 60 * 1000;
  const millisecondsSinceRangeEnd =
    -parseInt(timeRange.timeRange[1]) * 60 * 1000;
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
            query: `${query}`,
            timeRange: {
              type: "relative",

              start: millisecondsSinceRangeStart,
              end: millisecondsSinceRangeEnd,
            },
          },
        },
      },
    });

    return () => {
      myChart.destroy();
    };
  }, [query, time, millisecondsSinceRangeStart, millisecondsSinceRangeEnd]);
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
  };
}

export default connect(mapStateToProps)(PrometheusGraph);
