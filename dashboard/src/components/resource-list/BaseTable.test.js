// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { cleanup, fireEvent, render } from '@testing-library/react';
import 'jest-canvas-mock';
import React from 'react';
import Resource from '../../api/resources/Resource';
import BaseTable from './BaseTable';
import { dataset_columns } from './DatasetTable';
import { entity_columns } from './EntityTable';
import { feature_columns } from './FeatureTable';
import { model_columns } from './ModelTable';
import { entity_list } from './testData/entity_list';
import { feature_list } from './testData/feature_list';
import { model_list } from './testData/model_list';
import { trainingset_list } from './testData/training_set_list';
import { trainingset_column } from './TrainingSetTable';

describe('Base Table Tests', () => {
  const LOADING_GRID_ID = 'loadingGrid';
  const defaultProps = {
    title: 'Test table',
    type: feature_list[0].type,
    columns: feature_columns,
    resources: feature_list,
    loading: false,
    redirect: jest.fn(),
  };

  const getTestBody = (props = defaultProps) => {
    return (
      <>
        <BaseTable {...props} />
      </>
    );
  };

  beforeEach(() => {
    jest.resetAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  test('Basic table renders no data message without resources', async () => {
    //given:
    const helper = render(getTestBody({ ...defaultProps, resources: [] }));
    const foundText = helper.getByText(
      `No ${Resource[defaultProps.type].typePlural} Registered`
    );

    //expect:
    expect(foundText).toBeDefined();
  });

  test('Basic table renders each resource row', async () => {
    //given:
    const props = { ...defaultProps };

    const helper = render(getTestBody(props));

    //then:
    props.resources.forEach((element) => {
      const foundTitle = helper.getByText(element.name);
      expect(foundTitle).toBeDefined();
    });
  });

  test('Table renders loading gird if loading prop is true', async () => {
    //given:
    const props = { ...defaultProps };
    props.loading = true;

    const helper = render(getTestBody(props));
    const foundLoading = helper.getByTestId(LOADING_GRID_ID);

    //then:
    expect(foundLoading).toBeDefined();
    expect(foundLoading.nodeName).toBe('DIV');
  });

  test('Clicking a row fires of the redirect function', async () => {
    //given:
    const props = { ...defaultProps };
    props.loading = false;
    const resourceName = props.resources[0].name;

    const helper = render(getTestBody(props));
    const foundRow = helper.getByText(resourceName);
    fireEvent.click(foundRow);

    //then:
    expect(props.redirect).toHaveBeenCalledTimes(1);
    expect(props.redirect).toHaveBeenCalledWith(resourceName);
  });

  test.each`
    TitleParam         | ColumnsParam          | ResourcesParam
    ${'Datasets'}      | ${dataset_columns}    | ${feature_list}
    ${'Entities'}      | ${entity_columns}     | ${entity_list}
    ${'Features'}      | ${feature_columns}    | ${feature_list}
    ${'Models'}        | ${model_columns}      | ${model_list}
    ${'Training Sets'} | ${trainingset_column} | ${trainingset_list}
  `(
    'The $TitleParam columns render the entire data set',
    async ({ TitleParam, ColumnsParam, ResourcesParam }) => {
      //given:
      const props = {
        ...defaultProps,
        type: feature_columns[0].type,
        columns: ColumnsParam,
        title: TitleParam,
        resources: ResourcesParam,
      };

      const helper = render(getTestBody(props));
      const foundTitle = helper.getByText(TitleParam);

      //then: the title is found and all resources load
      expect(foundTitle).toBeDefined();
      props.resources.forEach((element) => {
        const foundTitle = helper.getByText(element.name);
        expect(foundTitle).toBeDefined();
      });
    }
  );
});
