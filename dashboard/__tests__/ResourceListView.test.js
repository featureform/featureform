import { cleanup, render } from '@testing-library/react';
import Adapter from '@wojtekmaj/enzyme-adapter-react-17';
import { configure } from 'enzyme';
import produce from 'immer';
import 'jest-canvas-mock';
import React from 'react';
import { ResourceListView } from '../src/components/resource-list/ResourceListView';
import { deepCopy } from '../src/helper';

configure({ adapter: new Adapter() });

describe('ResourceListView tests', () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  const NO_RECORDS = 'No records to display';
  const PROGRESS_BAR = 'progressbar';
  const SVG_NODE = 'svg';
  const DIV_NODE = 'DIV';
  const TD_NODE = 'TD';

  test('The resource list renders correctly when no data is present', () => {
    //given:
    const helper = render(<ResourceListView title='test' type='Feature' />);

    //when:
    const foundNoRecords = helper.getByText(NO_RECORDS);

    //then:
    expect(foundNoRecords.nodeName).toBe(TD_NODE);
  });

  test('The resouce list correctly renders the name and description columns', () => {
    //given: a row with data
    const helper = render(
      <ResourceListView
        title='test'
        type='Feature'
        resources={[
          {
            name: 'abc',
            description: 'my description',
            revision: 'Invalid Date',
            'default-variant': 'first-variant',
          },
        ]}
      />
    );

    //when:
    const foundName = helper.getByText('abc');
    const foundDesc = helper.getByText('my description');

    //then:
    expect(foundName.nodeName).toBe(TD_NODE);
    expect(foundDesc.nodeName).toBe(TD_NODE);
  });

  test('deepCopy takes an immutable object and makes it mutable', () => {
    //given:
    const immutData = produce([], (draft) => {
      draft.push({
        name: 'abc',
        'default-variant': 'first-variant',
        variants: { 'first-variant': {}, 'second-variant': {} },
      });
    });

    //when:
    const mutableCopy = deepCopy(immutData);
    mutableCopy[0].name = 'change from the original name';

    //then:
    expect(Object.isFrozen(immutData)).toBeTruthy();
    expect(immutData[0].name).not.toBe(mutableCopy[0].name);
    expect(Object.isExtensible(mutableCopy)).toBeTruthy();
  });

  test('When resources are empty, set isLoading to true and verify the progress bar', async () => {
    //given:
    const helper = render(<ResourceListView title='test' type='Feature' />);

    //when:
    const foundNoRecords = await helper.findByText(NO_RECORDS);
    const foundProgressBar = await helper.findByRole(PROGRESS_BAR);

    //then:
    expect(foundNoRecords.nodeName).toBe(TD_NODE);
    expect(foundProgressBar.nodeName).toBe(DIV_NODE);
    expect(foundProgressBar.firstChild.nodeName).toBe(SVG_NODE);
  });

  test('When the loading prop is true, ensure the progress bar is rendered', async () => {
    const helper = render(
      <ResourceListView title='test' type='Feature' loading={true} />
    );
    const foundNoRecords = await helper.findByText(NO_RECORDS);
    const foundProgressBar = await helper.findByRole(PROGRESS_BAR);

    expect(foundNoRecords.nodeName).toBe(TD_NODE);
    expect(foundProgressBar.nodeName).toBe(DIV_NODE);
    expect(foundProgressBar.firstChild.nodeName).toBe(SVG_NODE);
  });

  test('When failed, set isLoading to true', async () => {
    const helper = render(
      <ResourceListView
        title='test'
        loading={false}
        failed={true}
        type='Feature'
      />
    );
    const foundNoRecords = await helper.findByText(NO_RECORDS);
    const foundProgressBar = await helper.findByRole(PROGRESS_BAR);

    expect(foundNoRecords.nodeName).toBe(TD_NODE);
    expect(foundProgressBar.nodeName).toBe(DIV_NODE);
    expect(foundProgressBar.firstChild.nodeName).toBe(SVG_NODE);
  });
});
