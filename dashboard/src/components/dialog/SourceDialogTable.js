import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from '@mui/material';
import * as React from 'react';

export default function SourceDialogTable({ columns = [], rowList = [] }) {
  return (
    <div>
      <TableContainer component={Paper}>
        <Table sx={{ minWidth: 300 }} aria-label='Source Data Table'>
          <TableHead>
            <TableRow>
              {columns.map((col, i) => (
                <TableCell
                  key={col + i}
                  data-testid={col + i}
                  align={i === 0 ? 'left' : 'right'}
                >
                  {col}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {rowList.map((currentRow, index) => (
              <TableRow
                key={'mainRow' + index}
                data-testid={'mainRow' + index}
                sx={{ '&:last-child td, &:last-child th': { border: 0 } }}
              >
                {currentRow.map((row, index) => (
                  <TableCell
                    key={row + index}
                    align={index === 0 ? 'left' : 'right'}
                  >
                    {row}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </div>
  );
}
