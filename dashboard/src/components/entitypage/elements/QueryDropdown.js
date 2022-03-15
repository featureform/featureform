import PrometheusGraph from "./PrometheusGraph";
import React from "react";
import { makeStyles } from "@material-ui/core/styles";

const useStyles = makeStyles((theme) => ({
  root: {
    maxHeight: "20em",
  },
}));
function QueryDropdown({ query, type, name, query_type, add_labels, remote }) {
  const classes = useStyles();
  console.log(remote);

  return (
    <div className={classes.root}>
      <PrometheusGraph
        query={query}
        type={type}
        name={name}
        query_type={query_type}
        add_labels={add_labels}
        remote={remote}
      />
    </div>
  );
}

export default QueryDropdown;
