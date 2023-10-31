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
            barPercentage: 1.25,
            backgroundColor: [
              'rgba(255, 99, 132, 0.2)',
              'rgba(255, 159, 64, 0.2)',
              'rgba(255, 205, 86, 0.2)',
              'rgba(75, 192, 192, 0.2)',
              'rgba(54, 162, 235, 0.2)',
              'rgba(153, 102, 255, 0.2)',
              'rgba(158, 102, 138, 0.2)',
              'rgba(201, 203, 207, 0.2)',
              'rgba(95, 95, 226, 0.2)',
              'rgba(158, 198, 200, 0.2)',
            ],
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
            yAxes: [{ ticks: { beginAtZero: true, maxTicksLimit: 3 } }],
            xAxes: [
              {
                ticks: {
                  beginAtZero: true,
                  maxTicksLimit: 3,
                },
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
