import { Chart } from 'chart.js';
import * as React from 'react';

export default function Barchart({
  categories = [],
  categoryCounts = [],
  type = 'numeric',
}) {
  const chartRef = React.useRef(null);

  let catList = categories.map((category) => {
    let catString = '';
    if (type === 'numeric') {
      let num1 = parseInt(category[0])?.toLocaleString();
      let num2 = parseInt(category[1])?.toLocaleString();
      catString = `${num1} - ${num2}`;
    } else if (type === 'boolean') {
      catString = category;
    }
    return catString;
  });

  React.useEffect(async () => {
    if (chartRef) {
      const data = {
        labels: catList,
        datasets: [
          {
            data: categoryCounts,
            borderWidth: 0,
            barPercentage: 1.3,
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
          tooltips: {
            mode: 'index',
            intersect: false,
          },
          hover: {
            mode: 'index',
            intersect: false,
          },
          scales: {
            yAxes: [{ ticks: { beginAtZero: true, display: false } }],
            xAxes: [
              {
                ticks: { beginAtZero: true, display: false },
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

  return (
    <div>
      <canvas ref={chartRef} height={'125px'} width={'125px'} />
    </div>
  );
}
