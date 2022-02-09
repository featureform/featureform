import PrometheusGraph from "./PrometheusGraph";
import React from "react";
import Select from "@material-ui/core/Select";
import MenuItem from "@material-ui/core/MenuItem";
import { makeStyles } from "@material-ui/core/styles";

const useStyles = makeStyles((theme) => ({
  root: {
    maxHeight: "20em",
  },
}));
function QueryDropdown({ query, type, name, query_type, add_labels }) {
  const classes = useStyles();

  const [agg, setAgg] = React.useState("sum");

  function handleChange(event) {
    setAgg(event.target.value);
  }
  return (
    <div className={classes.root}>
      <PrometheusGraph
        query={query}
        type={type}
        name={name}
        query_type={query_type}
        add_labels={add_labels}
      />
    </div>
  );
}

export default QueryDropdown;
