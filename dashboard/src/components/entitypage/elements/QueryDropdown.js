//http://localhost:9090/api/v1/label/__name__/values

import AsyncSelect from "react-select/async";
import {
  Select,
  MenuItem,
  Box,
  FormControl,
  InputLabel,
  Container,
  Typography,
} from "@material-ui/core/";

import PrometheusGraph from "./PrometheusGraph";
import React, { useState, useEffect } from "react";
function QueryDropdown() {
  // handle input change event
  //   const handleInputChange = (value) => {
  //     setValue(value);
  //   };

  // handle selection
  //   const handleChange = (value) => {
  //     setSelectedValue(value);
  //   };

  // load options using API call
  const loadOptions = (inputValue) => {
    return fetch(`http://localhost:9090/api/v1/label/__name__/values`)
      .then((res) => res.json())
      .then((data) => {
        setOptions(data.data);
      });
  };
  const [age, setAge] = React.useState(0);
  useEffect(() => {
    loadOptions();
  }, [age]);

  const [options, setOptions] = React.useState(["first", "second", "third"]);

  const handleChange = (event) => {
    setAge(event.target.value);
  };

  return (
    <div>
      <Box sx={{ minWidth: 240 }}>
        <FormControl fullWidth>
          <InputLabel id="demo-simple-select-label">Metric Select</InputLabel>
          <Select
            labelId="demo-simple-select-label"
            id="demo-simple-select"
            value={age}
            label="Metrics Options"
            onChange={handleChange}
          >
            {options.map((option, i) => (
              <MenuItem value={option}>{option}</MenuItem>
            ))}
          </Select>
        </FormControl>
      </Box>

      <Container>
        <PrometheusGraph query={age} />
      </Container>
      {/* <AsyncSelect
        cacheOptions
        defaultOptions
        value={selectedValue}
        loadOptions={loadOptions}
        onInputChange={handleInputChange}
        onChange={handleChange}
      />
      <pre>Selected Value: {JSON.stringify(selectedValue || {}, null, 2)}</pre> */}
    </div>
  );
}

export default QueryDropdown;
