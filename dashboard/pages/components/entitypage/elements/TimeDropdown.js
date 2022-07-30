import React from "react";
import { connect } from "react-redux";
import { changeTime } from "./ExponentialTimeSliderSlice.js";
import Box from "@mui/material/Box";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import FormControl from "@mui/material/FormControl";
import Select from "@mui/material/Select";

const dropdownValues = [
  { label: "1h", value: 60 },
  { label: "1d", value: 360 * 4 },
  { label: "1w", value: 360 * 4 * 7 },
];

function ExponentialTimeSlider({ timeRange, changeTime }) {
  const [time, setTime] = React.useState(60);

  const handleChange = (event) => {
    setTime(event.target.value);
    changeTime([event.target.value, 0]);
  };

  return (
    <div>
      <Box sx={{ minWidth: 120 }}>
        <FormControl fullWidth>
          <InputLabel id="demo-simple-select-label">Time Select</InputLabel>
          <Select
            labelId="demo-simple-select-label"
            id="demo-simple-select"
            value={time}
            label="Time Select"
            onChange={handleChange}
          >
            {/* {Object.keys(resourceData).map((key, i) => ( */}
            {dropdownValues.map((item) => (
              <MenuItem key={item.label} value={item.value}>
                {item.label}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </Box>
    </div>
  );
}

function mapStateToProps(state) {
  return {
    timeRange: state.timeRange,
  };
}

const mapDispatchToProps = (dispatch) => {
  return {
    changeTime: (timeRange) => dispatch(changeTime({ timeRange })),
  };
};

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(ExponentialTimeSlider);
