// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package stringset

import (
	"testing"
)

func TestStringSetContains(t *testing.T) {
	setA := StringSet{"a": true, "b": true, "c": false}
	setB := StringSet{"a": true, "c": false}

	expected := true
	actual := setA.Contains(setB)

	if expected != actual {
		t.Errorf("Expected set A to contain set B, but instead received: %v", actual)
	}
}

func TestStringSetDoesNotContain(t *testing.T) {
	setA := StringSet{"a": true, "c": false}
	setB := StringSet{"a": true, "b": true, "c": false}

	expected := false
	actual := setA.Contains(setB)

	if expected != actual {
		t.Errorf("Expected set A not to contain set B, but instead received: %v", actual)
	}
}

func TestStringSetEmptySetA(t *testing.T) {
	setA := StringSet{}
	setB := StringSet{"a": true, "c": false}

	expected := false
	actual := setA.Contains(setB)

	if expected != actual {
		t.Errorf("Expected empty set A not to contain set B, but instead received: %v", actual)
	}
}

func TestStringSetEmptySetB(t *testing.T) {
	setA := StringSet{"a": true, "c": false}
	setB := StringSet{}

	expected := true
	actual := setA.Contains(setB)

	if expected != actual {
		t.Errorf("Expected set A to contain empty set B, but instead received: %v", actual)
	}
}

func TestStringSetDifference(t *testing.T) {
	setA := StringSet{"a": true, "b": true, "c": false}
	setB := StringSet{"a": true, "c": false}

	expected := StringSet{"b": true}
	actual := setA.Difference(setB)

	if !actual.Contains(expected) {
		t.Errorf("Expected difference to be %v, but instead received: %v", expected, actual)
	}
}
