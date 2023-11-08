import React from 'react';

const VariantNotFound = ({ type = '', entity = '', queryVariant = '' }) => {
  return (
    <div data-testid='variantNotFoundId'>
      <h1>404: Variant not found</h1>
      {type && entity && queryVariant && (
        <p>
          For &quot;<strong>{`${type}: ${entity}`}</strong>&quot; no variant
          named &quot;
          <strong>{queryVariant}</strong>&quot; exists.
        </p>
      )}
    </div>
  );
};

export default VariantNotFound;
