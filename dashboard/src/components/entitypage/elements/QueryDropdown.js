import PrometheusGraph from "./PrometheusGraph";
import React from "react";
import { makeStyles } from "@material-ui/core/styles";

const useStyles = makeStyles((theme) => ({
  root: {
    maxHeight: "10em",
  },
}));
function QueryDropdown({ query, type, name, query_type, add_labels }) {
  const classes = useStyles();

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
