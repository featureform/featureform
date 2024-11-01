// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import {
  default as reducer,
  setVariant,
} from '../src/components/resource-list/VariantSlice';

describe('VariantSlice', () => {
  it('sets variant', () => {
    const type = 'Feature';
    const name = 'abc';
    const variant = 'v1';
    const payload = { type, name, variant };
    const action = setVariant(payload);
    const newState = reducer(undefined, action);
    expect(newState).toMatchObject({ [type]: { [name]: variant } });
  });
});
