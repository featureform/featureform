import {
  Select,
  MenuItem,
  Box,
  FormControl,
  InputLabel,
} from "@material-ui/core/";
import Typography from "@material-ui/core/Typography";

import MetricsAPI from "api/resources/Metrics.js";
import {
  modifyMetrics,
  modifyInstances,
  fetchMetrics,
} from "./MetricsSelectSlice.js";

import React, { useEffect } from "react";
import { connect } from "react-redux";

const metricsAPI = new MetricsAPI();

const mapDispatchToProps = (dispatch) => {
  return {
    fetchMetrics: (api) => dispatch(fetchMetrics({ api })),
    modifyMetrics: (selection) => dispatch(modifyMetrics({ selection })),
    setInstances: (instances) => dispatch(modifyInstances({ instances })),
  };
};

function mapStateToProps(state) {
  return {
    metricsSelect: state.metricsSelect,
  };
}

function MetricsSelect({ metricsSelect, modifyMetrics, fetchMetrics }) {
  const [selection, setSelection] = React.useState("");
  const options = metricsSelect.resources ? metricsSelect.resources : {};
  const metrics = Object.keys(options);
  const metricsDesc =
    options && selection !== "" ? options[selection][0]["help"] : "";

  useEffect(() => {
    fetchMetrics(metricsAPI);
  }, [fetchMetrics]);

  const handleChange = (event) => {
    modifyMetrics(event.target.value);
    if (selection !== event.target.value) {
      setSelection(event.target.value);
    }
  };

  return (
    <div>
      <div>
        <Box sx={{ minWidth: 240 }}>
          <FormControl fullWidth>
            <InputLabel id="demo-simple-select-label">Metric Select</InputLabel>
            <Select
              labelId="demo-simple-select-label"
              id="demo-simple-select"
              value={selection}
              label="Metrics Options"
              onChange={handleChange}
            >
              {metrics.map((option) => (
                <MenuItem value={option}>{option}</MenuItem>
              ))}
            </Select>
            <Typography>{metricsDesc}</Typography>
          </FormControl>
        </Box>
      </div>
    </div>
  );
}

export default connect(mapStateToProps, mapDispatchToProps)(MetricsSelect);
