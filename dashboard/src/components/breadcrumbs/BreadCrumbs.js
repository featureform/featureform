// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import NavigateNextIcon from '@mui/icons-material/NavigateNext';
import Breadcrumbs from '@mui/material/Breadcrumbs';
import { styled } from '@mui/system';
import Link from 'next/link';
import { useRouter } from 'next/router';
import React from 'react';

const Root = styled('div')({
  margin: 5,
});

const StyledBreadcrumbs = styled(Breadcrumbs)({
  fontSize: 18,
});

const Separator = styled(NavigateNextIcon)({
  marginLeft: '0.2em',
  marginRight: '0.2em',
  alignItems: 'auto',
});

const BreadCrumbs = () => {
  const { asPath } = useRouter();
  const sansQuery = asPath.split('?').shift();
  const path = sansQuery.split('/');
  while (path.length > 0 && path[0].length === 0) {
    path.shift();
  }

  const capitalize = (word) => {
    return word ? word[0].toUpperCase() + word.slice(1).toLowerCase() : '';
  };

  const pathBuilder = (accumulator, currentValue) =>
    accumulator + '/' + currentValue;

  return (
    <Root>
      {path.length > 0 ? (
        <StyledBreadcrumbs
          style={{ margin: '0.25em' }}
          aria-label='breadcrumb'
          separator={<Separator fontSize='medium' />}
        >
          <Link href='/'>Home</Link>
          {path.map((ent, i) => (
            <Link
              key={`link-${i}`}
              href={'/' + path.slice(0, i + 1).reduce(pathBuilder)}
            >
              {i === path.length - 1 ? (
                <b>{capitalize(ent)}</b>
              ) : (
                capitalize(ent)
              )}
            </Link>
          ))}
        </StyledBreadcrumbs>
      ) : (
        <div></div>
      )}
    </Root>
  );
};

export default BreadCrumbs;
