// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package stringset

type nothing struct{}

type Ordered struct {
	list []string
	seen map[string]nothing
}

func NewOrdered(initial ...string) *Ordered {
	orderedSet := &Ordered{
		list: make([]string, 0, len(initial)),
		seen: make(map[string]nothing),
	}
	// Need to use this method to ignore duplicates in the initial set.
	orderedSet.AddAndGetDuplicates(initial...)
	return orderedSet
}

func (set *Ordered) AddAndGetDuplicates(strs ...string) []string {
	duplicates := make([]string, 0)
	for _, str := range strs {
		if _, has := set.seen[str]; has {
			duplicates = append(duplicates, str)
			continue
		}
		set.seen[str] = nothing{}
		set.list = append(set.list, str)
	}
	return duplicates
}

func (set *Ordered) ToList() []string {
	listCopy := make([]string, len(set.list))
	copy(listCopy, set.list)
	return listCopy
}
