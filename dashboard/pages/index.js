// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import Head from 'next/head';
import React from 'react';
import HomePage from '../src/components/homepage/HomePage';

const IndexPage = () => {
  return (
    <div>
      <Head>
        <title>Featureform Dashboard</title>
      </Head>
      <HomePage />
    </div>
  );
};

export default IndexPage;
