import { Chip } from '@mui/material';
import React from 'react';

export default function StatusChip({ status }) {
  const chipMap = {
    SUCCESS: '#D1FAD0',
    PENDING: '#FAECD0',
    FAILED: '#FAD0F6',
    RUNNING: '#FFA500',
  };
  return (
    <Chip
      sx={{ backgroundColor: chipMap[status] ?? '#FFFFFF' }}
      size='small'
      label={status}
    />
  );
}
