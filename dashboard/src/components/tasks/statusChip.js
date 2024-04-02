import { Chip } from '@mui/material';
import React from 'react';

export default function StatusChip({ status }) {
  const chipMap = {
    3: '#D1FAD0',
    2: '#FAECD0',
    4: '#FAD0F6',
    5: '#FFA500',
  };
  const statusMap = {
    3: 'Ready',
    2: 'Pending',
    4: 'Failed',
    5: 'Running',
  };
  return (
    <Chip
      sx={{ backgroundColor: chipMap[status] ?? '#FFFFFF' }}
      size='small'
      label={statusMap[status] ?? 'No Status'}
    />
  );
}
