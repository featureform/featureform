// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import Slider from '@mui/material/Slider';
import Typography from '@mui/material/Typography';
import { styled } from '@mui/system';
import React, { useState } from 'react';
import { connect } from 'react-redux';
import { changeTime } from './ExponentialTimeSliderSlice.js';

const DateRangeView = styled('div')({});

const minutesSince = [
  {
    value: 0,
    scaledValue: 2620000,
    label: '~',
  },
  {
    value: 25,
    scaledValue: 262800,
    label: '6mo',
  },
  {
    value: 50,
    scaledValue: 43800,
    label: '1mo',
  },
  {
    value: 75,
    scaledValue: 10080,
    label: '1w',
  },
  {
    value: 100,
    scaledValue: 1440,
    label: '1d',
  },
  {
    value: 125,
    scaledValue: 60,
    label: '1h',
  },
  {
    value: 150,
    scaledValue: 10,
    label: '10m',
  },
  {
    value: 175,
    scaledValue: 1,
    label: '1m',
  },
  {
    value: 200,
    scaledValue: 0,
    label: 'now',
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
function ExponentialTimeSlider({ changeTime }) {
  function convToDateTime(value) {
    let d = new Date(Date.now() - 1000 * 60 * value);

    return d.toUTCString();
  }
  const [value, setValue] = useState([175, 200]);

  const handleChange = (event, newValue) => {
    setValue(newValue);
  };

  const dispatchChange = (event, newValue) => {
    setValue(newValue);
    let newTimeRange = newValue.map((val) => val);
    changeTime(scaleValues(newTimeRange));
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
        onChangeCommitted={dispatchChange}
        valueLabelDisplay='auto'
      />
      <DateRangeView>
        {scaleValues(value).map((value, i) => (
          <Typography key={i} variant='body2'>
            {convToDateTime(value)}
          </Typography>
        ))}
      </DateRangeView>
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
