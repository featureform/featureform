import React from "react";
import { useEffect } from "react";
import { ResponsiveLine } from "@nivo/line";
import { Chart } from "chart.js";
import { ChartDatasourcePrometheusPlugin } from "chartjs-plugin-datasource-prometheus";
import {
  Select,
  MenuItem,
  Box,
  FormControl,
  InputLabel,
  Container,
  Typography,
} from "@material-ui/core/";

const PrometheusGraph = ({ query, time }) => {
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

              // from 12 hours ago to now
              start: -12 * 60 * 60 * 1000,
              end: 0,
            },
          },
        },
      },
    });

    return () => {
      myChart.destroy();
    };
  }, [query, time]);
  const chartRef = React.useRef(null);

  return (
    <div>
      <canvas ref={chartRef} />
    </div>
  );
};

export default PrometheusGraph;
