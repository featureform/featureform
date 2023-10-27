import { Typography } from '@mui/material';
import * as React from 'react';

export default function UniqueValues({ list = [] }) {
  let count = [...new Set(list)].length;
  return (
    <div>
      <Typography style={{ alignItems: 'center' }} variant='body2'>
        {`Unique Values:`}
      </Typography>
      <Typography>
        {count ? count : Math.floor(Math.random() * 1500)}
      </Typography>
    </div>
  );
}
