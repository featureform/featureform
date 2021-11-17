import { Container } from "@material-ui/core/";

import PrometheusGraph from "./PrometheusGraph";
import React from "react";
function QueryDropdown() {
  return (
    <div>
      <Container>
        <PrometheusGraph />
      </Container>
    </div>
  );
}

export default QueryDropdown;
