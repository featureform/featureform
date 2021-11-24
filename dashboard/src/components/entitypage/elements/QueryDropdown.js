import { Container } from "@material-ui/core/";

import PrometheusGraph from "./PrometheusGraph";
import React from "react";
function QueryDropdown({ type, name, query_type, add_labels }) {
  return (
    <div>
      <Container>
        <PrometheusGraph
          type={type}
          name={name}
          query_type={query_type}
          add_labels={add_labels}
        />
      </Container>
    </div>
  );
}

export default QueryDropdown;
