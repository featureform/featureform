import React from 'react';
import TableDataWrapper from './tableDataWrapper';

export default function JobPage() {
  /*
    todox:
    
    need to create title component

    wrap presentation component in data provider

    all / active / complete options

    need a table component with the various columns

    popover view with metadata details (shouldn't require a load from server)



    */
  return (
    <>
      <h1>Task Runs</h1>
      <TableDataWrapper />
    </>
  );
}
