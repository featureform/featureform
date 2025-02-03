// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package stringset

type StringSet map[string]bool

func (a StringSet) Add(items ...string) {
	for _, item := range items {
		a[item] = true
	}
}

func (a StringSet) Contains(b StringSet) bool {
	for str := range b {
		if _, ok := a[str]; !ok {
			return false
		}
	}
	return true
}

// Difference returns a new StringSet containing elements in a that are not in b.
// For example: a = {"a": true, "b": true}, b = {"b": true} => diff = {"a": true}
func (a StringSet) Difference(b StringSet) StringSet {
	diff := make(StringSet)
	for str := range a {
		if _, ok := b[str]; !ok {
			diff[str] = true
		}
	}
	return diff
}

func (a StringSet) List() []string {
	list := make([]string, 0, len(a))
	for str := range a {
		list = append(list, str)
	}
	return list
}
