// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { useRouter } from 'next/router';
import React from 'react';
import ResourcesAPI from '../../../src/api/resources/Resources';
import EntityPage from '../../../src/components/entitypage/EntityPage';

const EntityPageRoute = () => {
  const router = useRouter();
  const { type, entity } = router.query;
  const { variant: queryVariant } = router.query;
  const apiHandle = new ResourcesAPI();

  return (
    <EntityPage
      api={apiHandle}
      type={type}
      entity={entity}
      queryVariant={queryVariant}
    />
  );
};

export default EntityPageRoute;
