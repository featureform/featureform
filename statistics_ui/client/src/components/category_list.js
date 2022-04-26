import React from 'react'

const CategoryList = (data) => {
  let category_list = []
  for (let i = 0; i < data.data.length; i++) {
    category_list.push(data.data[i].category);
  }
    return (
      <div>
        { category_list }
      </div>
    )
}

export default CategoryList