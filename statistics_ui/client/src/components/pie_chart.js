import React, {useState, useEffect} from 'react';
import { Pie } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ArcElement,
  } from 'chart.js';
  
  ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ArcElement
  );

const PieChart = (data) => {
  let percentage_list = []
  let label_list = []
  for (let i = 0; i < data.data.length; i++) {
    label_list.push(data.data[i].variable);
    percentage_list.push(data.data[i].value);
  }
    return (
      <div>
        <Pie
          data = {{
            labels: label_list, //['Red', 'Blue', 'Yellow', 'Green', 'Purple', 'Orange'],
            datasets: [{
              label: "Test",
              data: percentage_list, //[65, 59, 80, 81, 56, 55, 40],
              backgroundColor: [
                '#B21F00',
                '#C9DE00',
                '#2FDE00',
                '#00A6B4',
                '#6800B4'
              ],
              hoverBackgroundColor: [
              '#501800',
              '#4B5000',
              '#175000',
              '#003350',
              '#35014F'
              ],
            }]
          }}
          height={400}
          width={600}
          options={{
            maintainAspectRatio: false,
            scales: {
              yAxes: [
                {
                  ticks: {
                    beginAtZero: true,
                  },
                },
              ],
            },
            legend:{
              display:true,
              position:'right'
            },
          }}
        />
      </div>
    )
        
  
}

export default PieChart