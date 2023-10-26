import { Chart } from 'chart.js';
import * as React from 'react';

export default function Barchart({
  categories = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'],
  categoryCounts = [20, 5, 7, 10, 15, 20, 25, 26, 27],
}) {
  categoryCounts;
  let dataList = [];
  for (let i = 0; i < categories.length; i++) {
    dataList[i] = Math.floor(Math.random() * 25 + 1);
  }
  React.useEffect(async () => {
    if (chartRef) {
      const data = {
        labels: categories,
        datasets: [
          {
            data: dataList,
            borderWidth: 0,
            barPercentage: 0.8,
          },
        ],
      };
      var myChart = new Chart(chartRef.current, {
        type: 'bar',
        data: data,
        options: {
          legend: {
            display: false,
          },
          scales: {
            yAxes: [{ ticks: { beginAtZero: true, display: false } }],
            // xAxes: [{ ticks: { display: false } }],
          },
        },
      });
    }
    return () => {
      myChart?.destroy();
    };
  }, [chartRef]);

  const chartRef = React.useRef(null);

  return (
    <div>
      <canvas ref={chartRef} height={'125px'} width={'125px'} />
    </div>
  );
}
