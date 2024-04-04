import { Chip } from '@mui/material';
import React from 'react';

export default function StatusChip({ status }) {
  const chipMap = {
    3: '#D1FAD0',
    2: '#FAECD0',
    1: '#FAECD0',
    4: '#FAD0F6',
    5: '#D6D0FA',
  };
  const STATUS_MAP = {
    0: 'No Status',
    1: 'Created',
    2: 'Pending',
    3: 'Ready',
    4: 'Failed',
    5: 'Running',
  };
  return (
    <Chip
      sx={{ backgroundColor: chipMap[status] ?? '#FFFFFF' }}
      size='small'
      label={STATUS_MAP[status] ?? 'No Status'}
    />
  );
}
