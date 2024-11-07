// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { Box, Container, Grid, Typography } from '@mui/material';
import { styled } from '@mui/system';
import faker from 'faker';
import React from 'react';
import { Bar } from 'react-chartjs-2';

const Root = styled(Box)(({ theme }) => ({
  flexGrow: 1,
  padding: theme.spacing(0),
  backgroundColor: theme.palette.background.paper,
  flexBasis: theme.spacing(0),
  flexDirection: 'row',
  '& > *': {
    padding: theme.spacing(0),
  },
}));

const ErrorBar = styled(Box)({
  width: '50%',
});

const BarGraph = styled(Box)({
  maxWidth: '40em',
});

const Graph = styled(Box)({
  height: '40em',
  alignItems: 'center',
  '& > *': {
    height: '40em',
  },
});

const exampleStats = {
  Feature: {
    'Average Latency': '42ms',
    '95 Latency': '57ms',
    '99 Latency': '99ms',
    'Mean Error Rate': '0.1922%',
  },
  'Feature Set': {
    'Average Latency': '65ms',
    '95 Latency': '78ms',
    '99 Latency': '105ms',
    'Mean Error Rate': '0.1922%',
  },
  Entity: {
    Features: 44,
    'Primary Provider': 'Snowflake',
  },
};

export const options = {
  plugins: {
    title: {
      display: true,
      text: 'Query Error Summary',
    },
  },
  responsive: false,
  scales: {
    x: {
      stacked: true,
    },
    y: {
      stacked: true,
    },
  },
};

const labels = ['Last 1000 queries', 'Last 7 days', 'Overall'];

export const data = {
  labels,
  datasets: [
    {
      label: 'Client Errors',
      data: labels.map(() => faker.datatype.number({ min: 0, max: 100 })),
      backgroundColor: 'rgb(255, 99, 132)',
    },
    {
      label: 'Server Errors',
      data: labels.map(() => faker.datatype.number({ min: 0, max: 100 })),
      backgroundColor: 'rgb(75, 192, 192)',
    },
    {
      label: 'Provider Errors',
      data: labels.map(() => faker.datatype.number({ min: 0, max: 100 })),
      backgroundColor: 'rgb(53, 162, 235)',
    },
  ],
};

const StatsDropdown = ({ type }) => {
  const stats = exampleStats[type];

  return (
    <Root>
      <Grid container spacing={0}>
        <Grid item xs={12} height='15em'>
          <Graph>
            <Container minheight={'800px'}>
              {Object.keys(stats).map((key, i) => (
                <Typography key={i} variant='body1'>
                  <b>{key}: </b>
                  {stats[key]}
                </Typography>
              ))}
              <Typography variant='body1'>
                <b>Error Stats:</b>
              </Typography>
              <ErrorBar>
                <Bar options={options} data={data} className={BarGraph} />
              </ErrorBar>
            </Container>
          </Graph>
        </Grid>
      </Grid>
    </Root>
  );
};

export default StatsDropdown;
