// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package lib

import mapset "github.com/deckarep/golang-set/v2"

type Orderable interface {
	LessThan(Orderable) bool
}

func QuickSortInPlace[A Orderable](as []A) {
	quickSort(as, 0, len(as)-1)
}

func quickSort[A Orderable](as []A, low int, high int) {
	if low < high {
		p := partition(as, low, high)
		quickSort(as, low, p-1)
		quickSort(as, p+1, high)
	}
}

func partition[A Orderable](as []A, low int, high int) int {
	pivot := as[high]
	i := low - 1
	for j := low; j <= high-1; j++ {
		if as[j].LessThan(pivot) {
			i++
			as[i], as[j] = as[j], as[i]
		}
	}
	as[i+1], as[high] = as[high], as[i+1]
	return i + 1
}

func Dedupe[A comparable](as []A) []A {
	s := mapset.NewSet[A]()
	for _, v := range as {
		s.Add(v)
	}
	return s.ToSlice()
}

// ToSet takes a slice of type A and returns a mapset.Set of type A.
// Example usage:
//
//	s := ToSet[int]([]int{1, 2, 3, 4, 5})
//	s.Add(6)
//	fmt.Println(s.Contains(3)) // true
func ToSet[A comparable](as []A) mapset.Set[A] {
	set := mapset.NewSet[A]()
	for _, item := range as {
		set.Add(item)
	}
	return set
}
