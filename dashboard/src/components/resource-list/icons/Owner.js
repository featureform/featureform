// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import React from 'react';

export const UserBubbleSvg = (props) => (
  <svg {...props} viewBox='0 0 14 14' xmlns='http://www.w3.org/2000/svg'>
    <circle cx='7' cy='7' r='7' fill='#4A90E2' />
    <text
      x='50%'
      y='40%'
      textAnchor='middle'
      dy='.5em'
      fill='white'
      fontSize='10'
    >
      {props.letter ?? ''}
    </text>
  </svg>
);
