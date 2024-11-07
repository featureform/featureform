// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

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
