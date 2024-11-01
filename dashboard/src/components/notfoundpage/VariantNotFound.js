// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

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
