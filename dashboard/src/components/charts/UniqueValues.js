import { Typography } from '@mui/material';
import * as React from 'react';

export default function UniqueValues({ count = -1 }) {
  return (
    <div>
      <Typography style={{ alignItems: 'center' }} variant='body2'>
        {`Unique Values:`}
      </Typography>
      <Typography>{count}</Typography>
    </div>
  );
}
