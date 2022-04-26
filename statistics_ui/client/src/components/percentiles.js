import React, {useState, useEffect} from 'react'
// import { useTable } from "react-table";

const Percentiles = ({ median, lowerquartile, upperquartile }) => {
      const data = [{ median_col: median, lowerquartile_col: lowerquartile, upperquartile_col: upperquartile}];
      return (
            <div className="Percentiles">
              <table>
                <tr>
                  <th>Median</th>
                  <th>Lower Quartile</th>
                  <th>Upper Quartile</th>
                </tr>
                {data.map((val, key) => {
                  return (
                    <tr key={key}>
                      <td>{val.median_col}</td>
                      <td>{val.lowerquartile_col}</td>
                      <td>{val.upperquartile_col}</td>
                    </tr>
                  )
                })}
              </table>
            </div>
          );
      }
      // const data = [
      //       {median_col: 12, lowerquartile_col: 13, upperquartile_col: 14}
      //     ]

//   function Percentiles() {
//       return (
//         <div className="Percentiles">
//           <table>
//             <tr>
//               <th>Median</th>
//               <th>Lower Quartile</th>
//               <th>Upper Quartile</th>
//             </tr>
//             {data.map((val, key) => {
//               return (
//                 <tr key={key}>
//                   <td>{val.median_col}</td>
//                   <td>{val.lowerquartile_col}</td>
//                   <td>{val.upperquartile_col}</td>
//                 </tr>
//               )
//             })}
//           </table>
//         </div>
//       );
//     }
  
export default Percentiles;