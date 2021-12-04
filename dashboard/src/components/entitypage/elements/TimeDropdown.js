import React from "react";
import Typography from "@material-ui/core/Typography";
import { makeStyles } from "@material-ui/core/styles";
import Slider from "@material-ui/core/Slider";
import { connect } from "react-redux";
import { changeTime } from "./ExponentialTimeSliderSlice.js";
import Box from "@mui/material/Box";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import FormControl from "@mui/material/FormControl";
import Select, { SelectChangeEvent } from "@mui/material/Select";

const useStyles = makeStyles((theme) => ({
  dateRangeView: {},
}));

const dropdownValues = [
  { label: "1m", value: 1 },
  { label: "5m", value: 5 },
  { label: "10m", value: 10 },
  { label: "30m", value: 30 },
  { label: "1h", value: 60 },
  { label: "6h", value: 360 },
  { label: "1d", value: 360 * 4 },
  { label: "1w", value: 360 * 4 * 7 },
];

function ExponentialTimeSlider({ timeRange, changeTime }) {
  const classes = useStyles();

  const [time, setTime] = React.useState(10);

  const handleChange = (event) => {
    setTime(event.target.value);
    changeTime([time, 0]);
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
