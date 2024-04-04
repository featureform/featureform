import { Chip } from '@mui/material';
import React from 'react';

export default function StatusChip({ status = '' }) {
  const chipMap = {
    NO_STATUS: '#D0FAEC',
    CREATED: '#FFFFFF',
    PENDING: '#FAECD0',
    READY: '#D1FAD0',
    FAILED: '#FAD0F6',
  };
  return (
    <Chip
      sx={{ backgroundColor: chipMap[status] ?? '#FFFFFF' }}
      size='medium'
      label={status?.toUpperCase()}
    />
  );
}
