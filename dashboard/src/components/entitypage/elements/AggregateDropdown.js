import React from "react";
import { connect } from "react-redux";
import Box from "@mui/material/Box";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import FormControl from "@mui/material/FormControl";
import Select from "@mui/material/Select";
import { changeAggregate } from "./AggregateDropdownSlice.js";

const dropdownValues = [
  { label: "Average", value: "avg" },
  { label: "Sum", value: "sum" },
  { label: "Minimum", value: "min" },
  { label: "Maximum", value: "max" },
];

function AggregateDropdwon({ graph, aggregates, changeAggregate }) {
  const [agg, setAgg] = React.useState(aggregates.aggregates[graph]);

  console.log(aggregates);
  const handleChange = (event) => {
    setAgg(event.target.value);
    changeAggregate(0, event.target.value);
  };

  return (
    <div>
      <Box sx={{ minWidth: 120 }}>
        <FormControl fullWidth>
          <InputLabel id="demo-simple-select-label">
            Aggregation Select
          </InputLabel>
          <Select
            labelId="demo-simple-select-label"
            id="demo-simple-select"
            value={agg}
            label="Aggregation Select"
            onChange={handleChange}
          >
            {/* {Object.keys(resourceData).map((key, i) => ( */}
            {dropdownValues.map((item) => (
              <MenuItem value={item.value}>{item.label}</MenuItem>
            ))}
          </Select>
        </FormControl>
      </Box>
    </div>
  );
}

function mapStateToProps(state) {
  return {
    aggregates: state.aggregates,
  };
}

const mapDispatchToProps = (dispatch) => {
  return {
    changeAggregate: (graph, aggregate) =>
      dispatch(changeAggregate({ graph, aggregate })),
  };
};

export default connect(mapStateToProps, mapDispatchToProps)(AggregateDropdwon);
