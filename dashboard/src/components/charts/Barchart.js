import { Chart } from 'chart.js';
import * as React from 'react';

export default function Barchart({ categories = [], categoryCounts = [] }) {
  React.useEffect(async () => {
    if (chartRef) {
      const data = {
        labels: categories,
        datasets: [
          {
            data: categoryCounts,
            borderWidth: 0,
            barPercentage: 0.8,
          },
        ],
      };
      var myChart = new Chart(chartRef.current, {
        type: 'bar',
        data: data,
        options: {
          responsive: true,
          legend: {
            display: false,
          },
          scales: {
            yAxes: [{ ticks: { beginAtZero: true, display: false } }],
            xAxes: [
              {
                barPercentage: 1.3,
                ticks: { beginAtZero: true, maxTicksLimit: 5 },
              },
            ],
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
