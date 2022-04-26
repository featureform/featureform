import React, {useState, useEffect} from 'react'
// import { useTable } from "react-table";

const Count = ({ numCategories }) => {
      const data = [{ num_cat: numCategories}];
      return (
            <div className="Count">
              <table>
                <tr>
                  <th>Number of Unique Categories</th>
                </tr>
                {data.map((val, key) => {
                  return (
                    <tr key={key}>
                      <td>{val.num_cat}</td>
                    </tr>
                  )
                })}
              </table>
            </div>
          );
      }
  
export default Count;