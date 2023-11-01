import React from 'react';

const VariantNotFound = ({ type = '', queryVariant = '' }) => {
  return (
    <div data-testid='variantNotFoundId'>
      <h1>404: Variant not found</h1>
      {type && queryVariant && (
        <p>
          For &quot;<strong>{type}</strong>&quot; type, no variant named &quot;
          <strong>{queryVariant}</strong>&quot; exists.
        </p>
      )}
    </div>
  );
};

export default VariantNotFound;
