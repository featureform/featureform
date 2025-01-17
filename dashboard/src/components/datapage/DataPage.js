// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import React from 'react';
import ResourcesAPI from '../../api/resources';
import Resource from '../../api/resources/Resource.js';
import { LoadingDots } from '../../components/entitypage/EntityPage';
import NotFound from '../notfoundpage/NotFound';
import DatasetTable from '../resource-list/DatasetTable/DatasetTable';
import FeatureVariantTable from '../resource-list/FeatureTable/FeatureVariantTable';
import LabelVariantTable from '../resource-list/LabelTable/LabelVariantTable';
import TrainingSetTable from '../resource-list/TrainingSetTable/TrainingSetTable';
import EntityTable from '../resource-list/EntityTable/EntityTable';
import ProviderTable from '../resource-list/ProviderTable/ProviderTable';
import ResourceList from '../resource-list/ResourceList';
import ModelTable from '../resource-list/ModelTable/ModelTable';

const apiHandle = new ResourcesAPI();

const DataPage = ({ type }) => {
  let resourceType = Resource.pathToType[type];
  let body = <></>;
  switch (true) {
    case type === undefined && resourceType === undefined:
      body = <LoadingDots />;
      break;
    case resourceType === 'Feature':
      body = <FeatureVariantTable />;
      break;
    case resourceType === 'Source':
      body = <DatasetTable />;
      break;
    case resourceType === 'Label':
      body = <LabelVariantTable />;
      break;
    case resourceType === 'Entity':
      body = <EntityTable />;
      break;
    case resourceType === 'Provider':
      body = <ProviderTable />;
      break;
    case resourceType === 'TrainingSet':
      body = <TrainingSetTable />;
      break;
    case resourceType === 'Model':
      body = <ModelTable />;
      break;
    case !!resourceType:
      body = <ResourceList api={apiHandle} type={resourceType} />;
      break;
    default:
      body = <NotFound />;
      break;
  }

  return <>{body}</>;
};

export default DataPage;
