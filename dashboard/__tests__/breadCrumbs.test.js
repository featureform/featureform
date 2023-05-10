import React from 'react';
import 'jest-canvas-mock';
import { cleanup, render } from '@testing-library/react';
import BreadCrumbs from '../src/components/breadcrumbs/BreadCrumbs';

const userRouterMock = {
  asPath: '/',
};

jest.mock('next/router', () => ({
  useRouter: () => userRouterMock,
}));

describe('Bread Crumb Tests', () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  const getTestBody = (pathParam = '/') => {
    userRouterMock.asPath = pathParam;
    return (
      <>
        <BreadCrumbs />
      </>
    );
  };

  test('Issue-762: The breadcrumb component renders the ULR path correctly.', async () => {
    //given:
    let urlPath = '/training-sets/fraud_training/last_model';
    const helper = render(getTestBody(urlPath));

    //when: we find the breadcrumb links
    const anchorLinks = helper.container.querySelectorAll('a');
    const finalBread = helper.container.querySelector('b');

    //then: the bread crumbs are constructed correctly
    expect(anchorLinks.length).toBe(3);
    expect(anchorLinks[0].textContent).toBe('Home');
    expect(anchorLinks[0].href).toBe('http://localhost/');

    expect(anchorLinks[1].textContent).toBe('Training-sets');
    expect(anchorLinks[1].href).toBe('http://localhost/training-sets');

    expect(anchorLinks[2].textContent).toBe('Fraud_training');
    expect(anchorLinks[2].href).toBe(
      'http://localhost/training-sets/fraud_training'
    );

    expect(finalBread.textContent).toBe('Last_model');
  });

  test('Issue-762: Query params are ignored', async () => {
    //given: a url path with a query param
    let queryParam = '?q=shouldNotDisplayParam';
    const helper = render(getTestBody(`/path1${queryParam}`));

    //when: we find the breadcrumb links
    const anchorLinks = helper.container.querySelectorAll('a');
    const finalBread = helper.container.querySelector('b');
    const foundQueryParam = helper.queryByText(queryParam);

    //then: the query param is not present
    expect(anchorLinks.length).toBe(1);
    expect(anchorLinks[0].href).toBe('http://localhost/');
    expect(anchorLinks[0].textContent).toBe('Home');
    expect(anchorLinks[0].href).toBe('http://localhost/');
    expect(finalBread.textContent).toBe('Path1');
    expect(foundQueryParam).toBeNull();
  });

  test.each`
    PathParam                      | LinkCountParam | LastLinkParam
    ${'/path'}                     | ${1}           | ${'http://localhost/'}
    ${'/path1/path2/model1'}       | ${3}           | ${'http://localhost/path1/path2'}
    ${'/path1/path2/path3/model1'} | ${4}           | ${'http://localhost/path1/path2/path3'}
  `(
    `Issue-762: The route path "$PathParam" has $LinkCountParam anchor links`,
    async ({ PathParam, LinkCountParam, LastLinkParam }) => {
      //given:
      userRouterMock.asPath = PathParam;
      const helper = render(getTestBody(PathParam));

      //when:
      const anchorLinks = helper.container.querySelectorAll('a');
      const lastAnchor = Array.from(anchorLinks).pop();

      //then:
      expect(anchorLinks.length).toBe(LinkCountParam);
      expect(lastAnchor.href).toBe(LastLinkParam);
    }
  );
});
