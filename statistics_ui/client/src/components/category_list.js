import React from 'react'
// import MaterialTable, { MTableBody, MTableHeader } from "material-table";
import {Container, makeStyles} from "@material-ui/core";

const useStyles = makeStyles((theme) => ({
  border: {
    background: "white",
    border: `2px solid`,
    borderRadius: "16px",
    width: theme.spacing(3),
  },
}));

const CategoryList = (data) => {
  let category_list = []
  for (let i = 0; i < data.data.length; i++) {
    category_list.push(data.data[i].category);
  }
  const classes = useStyles()
    return (
      <div>
        <Container maxWidth="xl" className={classes.border}>
          { category_list }
        </Container>
      </div>
    )
}

export default CategoryList