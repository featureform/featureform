// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import {
  default as reducer,
  toggleTag,
} from '../src/components/resource-list/TagSlice';

describe('TagSlice', () => {
  const type = 'Feature';
  it('toggle on tag', () => {
    const tag = 'abc';
    const payload = { type, tag };
    const action = toggleTag(payload);
    const newState = reducer(undefined, action);
    expect(newState).toMatchObject({ [type]: { [tag]: true } });
  });

  it('toggle off tag', () => {
    const tag = 'abc';
    const payload = { type, tag };
    const action = toggleTag(payload);
    const toggleOn = reducer(undefined, action);
    const toggleOff = reducer(toggleOn, action);
    expect(toggleOff).toMatchObject({ [type]: {} });
  });
});
