// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package stringset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type orderedStringSetTest struct {
	Name    string
	Initial []string
	Steps   []addAndGetDuplicateStep
	Final   []string
}

func (test orderedStringSetTest) RunParallel(t *testing.T) {
	t.Run(test.Name, func(t *testing.T) {
		t.Parallel()
		set := NewOrdered(test.Initial...)
		for _, step := range test.Steps {
			step.Apply(t, set)
		}
		finalList := set.ToList()
		assert.Equal(
			t, finalList, test.Final,
			"got %v\nexpected %v",
			finalList, test.Final,
		)
	})
}

type addAndGetDuplicateStep struct {
	Add        []string
	Duplicates []string
}

func (step addAndGetDuplicateStep) Apply(t *testing.T, set *Ordered) {
	duplicates := set.AddAndGetDuplicates(step.Add...)
	assert.Equal(
		t, duplicates, step.Duplicates,
		"When adding %v to %v. Expected %v got %v",
		step.Add, set, step.Duplicates, duplicates,
	)
}

func TestOrderedStringSet(t *testing.T) {
	tests := []orderedStringSetTest{
		{
			Name:    "nil test",
			Initial: []string{},
			Final:   []string{},
		},
		{
			Name:    "simple initialize",
			Initial: []string{"a", "b", "c"},
			Final:   []string{"a", "b", "c"},
		},
		{
			Name:    "simple append",
			Initial: []string{},
			Steps: []addAndGetDuplicateStep{
				{
					Add:        []string{"a", "b", "c"},
					Duplicates: []string{},
				},
			},
			Final: []string{"a", "b", "c"},
		},
		{
			Name:    "initialize with duplicates",
			Initial: []string{"a", "b", "b"},
			Final:   []string{"a", "b"},
		},
		{
			Name:    "append no duplicates",
			Initial: []string{},
			Steps: []addAndGetDuplicateStep{
				{
					Add:        []string{"a"},
					Duplicates: []string{},
				},
				{
					Add:        []string{"b", "c"},
					Duplicates: []string{},
				},
			},
			Final: []string{"a", "b", "c"},
		},
		{
			Name:    "complex duplicate handling",
			Initial: []string{"a"},
			Steps: []addAndGetDuplicateStep{
				{
					Add:        []string{"a"},
					Duplicates: []string{"a"},
				},
				{
					Add:        []string{"a", "a"},
					Duplicates: []string{"a", "a"},
				},
				{
					Add:        []string{"a", "b"},
					Duplicates: []string{"a"},
				},
				{
					Add:        []string{"b", "a", "c"},
					Duplicates: []string{"b", "a"},
				},
			},
			Final: []string{"a", "b", "c"},
		},
	}
	for _, test := range tests {
		test.RunParallel(t)
	}
}
