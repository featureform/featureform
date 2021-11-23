import { Container } from "@material-ui/core/";

import PrometheusGraph from "./PrometheusGraph";
import React from "react";
function QueryDropdown({ type, name }) {
  return (
    <div>
      <Container>
        <PrometheusGraph type={type} name={name} />
      </Container>
    </div>
  );
}

export default QueryDropdown;
