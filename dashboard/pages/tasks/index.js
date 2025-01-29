// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import React from 'react';
import { useRouter } from 'next/router';
import TaskRunsPage from '../../src/components/tasks';

const TaskRunsPageRoute = () => {
  const router = useRouter();
  const { name, variant } = router.query;
  return <TaskRunsPage name={name} variant={variant}/>;
};

export default TaskRunsPageRoute;
