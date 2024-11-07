// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package stringset

type StringSet map[string]bool

func (a StringSet) Contains(b StringSet) bool {
	for str := range b {
		if _, ok := a[str]; !ok {
			return false
		}
	}
	return true
}
