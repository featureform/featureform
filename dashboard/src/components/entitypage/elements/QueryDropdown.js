import PrometheusGraph from "./PrometheusGraph";
import React from "react";
function QueryDropdown({ query, type, name, query_type, add_labels }) {
  return (
    <PrometheusGraph
      query={query}
      type={type}
      name={name}
      query_type={query_type}
      add_labels={add_labels}
    />

  );
}

export default QueryDropdown;
