import React from 'react';

const NotFound = ({ type = '', entity = '' }) => {
  return (
    <div data-testid='notFoundId'>
      <h1>404: Resource not found</h1>
      {entity && type && (
        <p>
          No resource of type: &quot;<strong>{type}</strong>&quot; and entity:
          &quot;
          <strong>{entity}</strong>&quot; exists.
        </p>
      )}
    </div>
  );
};

export default NotFound;
