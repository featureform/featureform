import React from "react";
import { useEffect } from "react";
import { ResponsiveLine } from "@nivo/line";
import { Chart } from "chart.js";
import { ChartDatasourcePrometheusPlugin } from "chartjs-plugin-datasource-prometheus";

const LatencyGraph = ({ data }) => {
return   <ResponsiveLine
data={data}
margin={{ top: 10, right: 110, bottom: 80, left: 60 }}
xScale={{ format: "%Y-%m-%dT%H:%M:%S.%L%Z", type: "time" }}
xFormat="time:%Y-%m-%dT%H:%M:%S.%L%Z"
yScale={{
  type: "linear",
  min: 0,
  max: "auto",
  stacked: true,
  reverse: false,
}}
axisTop={null}
axisRight={null}
axisBottom={{
  tickValues: "every 2 minutes",
  tickSize: 5,
  tickPadding: 5,
  tickRotation: 0,
  format: "%H:%M",
  legendOffset: 36,
  legendPosition: "middle",
}}
axisLeft={{
  orient: "left",
  tickSize: 5,
  tickPadding: 5,
  tickRotation: 0,
  legend: "milliseconds",
  legendOffset: -40,
  legendPosition: "middle",
}}
pointSize={5}
pointColor={{ theme: "background" }}
pointBorderWidth={2}
pointBorderColor={{ from: "serieColor" }}
pointLabelYOffset={-12}
useMesh={true}
legends={[
  {
    anchor: "bottom-right",
    direction: "column",
    justify: false,
    translateX: 100,
    translateY: 0,
    itemsSpacing: 0,
    itemDirection: "left-to-right",
    itemWidth: 80,
    itemHeight: 20,
    itemOpacity: 0.75,
    symbolSize: 12,
    symbolShape: "circle",
    symbolBorderColor: "rgba(0, 0, 0, .5)",
    effects: [
      {
        on: "hover",
        style: {
          itemBackground: "rgba(0, 0, 0, .03)",
          itemOpacity: 1,
        },
      },
    ],
  },
]}
/>
); 
};



export default LatencyGraph;
