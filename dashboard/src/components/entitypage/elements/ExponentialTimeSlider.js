import React from "react";
import Typography from "@material-ui/core/Typography";
import { makeStyles } from "@material-ui/core/styles";
import Slider from "@material-ui/core/Slider";

const useStyles = makeStyles((theme) => ({
  dateRangeView: {},
}));

const minutesSince = [
  {
    value: 0,
    scaledValue: 2620000,
    label: "~",
  },
  {
    value: 25,
    scaledValue: 262800,
    label: "6mo",
  },
  {
    value: 50,
    scaledValue: 43800,
    label: "1mo",
  },
  {
    value: 75,
    scaledValue: 10080,
    label: "1w",
  },
  {
    value: 100,
    scaledValue: 1440,
    label: "1d",
  },
  {
    value: 125,
    scaledValue: 60,
    label: "1h",
  },
  {
    value: 150,
    scaledValue: 10,
    label: "10m",
  },
  {
    value: 175,
    scaledValue: 1,
    label: "1m",
  },
  {
    value: 200,
    scaledValue: 0,
    label: "now",
  },
];

const scaleValues = (valueArray) => {
  return [scale(valueArray[0]), scale(valueArray[1])];
};
const scale = (value) => {
  if (value === undefined) {
    return undefined;
  }
  const previousMarkIndex = Math.floor(value / 25);
  const previousMark = minutesSince[previousMarkIndex];
  const remainder = value % 25;
  if (remainder === 0) {
    return previousMark.scaledValue;
  }
  const nextMark = minutesSince[previousMarkIndex + 1];
  const increment = (nextMark.scaledValue - previousMark.scaledValue) / 25;
  return remainder * increment + previousMark.scaledValue;
};

function numFormatter(value) {
  return value;
}

export default function ExponentialTimeSlider() {
  const classes = useStyles();

  function convToDateTime(value) {
    let d = new Date(Date.now() - 1000 * 60 * value);

    return d.toUTCString();
  }
  const [value, setValue] = React.useState([175, 200]);

  const handleChange = (event, newValue) => {
    setValue(newValue);
  };

  return (
    <div>
      <Slider
        style={{ maxWidth: 500 }}
        value={value}
        min={0}
        step={1}
        max={200}
        valueLabelFormat={(value) => <div>{numFormatter(value)}</div>}
        marks={minutesSince}
        scale={scaleValues}
        onChange={handleChange}
        valueLabelDisplay="auto"
      />
      <div className={classes.dateRangeView}>
        {scaleValues(value).map((value) => (
          <Typography variant="body2">{convToDateTime(value)}</Typography>
        ))}
      </div>
    </div>
  );
}
