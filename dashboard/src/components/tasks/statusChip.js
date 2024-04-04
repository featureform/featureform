import { Chip } from '@mui/material';
import React from 'react';

export default function StatusChip({ status }) {
  const chipMap = {
    3: '#D1FAD0',
    2: '#FAECD0',
    1: '#FAECD0',
    4: '#FAD0F6',
    5: '#FFA500',
  };
  const STATUS_MAP = {
    0: 'NO_STATUS',
    1: 'CREATED',
    2: 'PENDING',
    3: 'READY',
    4: 'FAILED',
    5: 'RUNNING',
  };
  return (
    <Chip
      sx={{ backgroundColor: chipMap[status] ?? '#FFFFFF' }}
      size='small'
      label={STATUS_MAP[status] ?? 'No Status'}
    />
  );
}
