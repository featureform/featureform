import React, {useState, useEffect} from 'react'
import {useParams} from "react-router-dom";
import Percentiles from './percentiles';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
  } from 'chart.js';
import BarChart from './bar_chart';
import PieChart from './pie_chart';
import Count from './count';
import CategoryList from './category_list'
  
  ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
  );

function Metrics() {

  const [metadata, setMetadata] = useState(0);
  const [alldata, setAllData] = useState(0);

  let {columnname} = useParams()

  useEffect(() => {
    const { REACT_APP_TABLE_DATA } = process.env;
    fetch(REACT_APP_TABLE_DATA, {
      method: 'post',
      headers: { 'Content-Type': "text/plain" },
      body: columnname,
    })
      .then((res) => res.json())
      .then((data) => {
        setMetadata(data.metadata.metric)
        setAllData(data)
      })
      .catch((err) => {
        console.log(err);
      });
  },[columnname, metadata]);


  if (alldata == 0) {
    return <div></div>
  } else if (metadata == "percentile") {
      return <div>
        <Percentiles
        median={alldata.median}
        lowerquartile={alldata.lowerquartile}
        upperquartile={alldata.upperquartile}/>
      </div>
  } else if (metadata == "bar") {
      return <div>
      <BarChart
      data={alldata.chartdata}/>
      </div>
  } else if (metadata == "percent") {
    return <div>
    <PieChart
    data={alldata.chartdata}/>
    </div>
  } else if (metadata == "count") {
    return <div>
    <Count
    numCategories={alldata.categorycount}/>
    </div>
  } else if (metadata == "categoryList") {
    return <div>
    <CategoryList
    data={alldata.categories}/>
    </div>
  }
}


export default Metrics