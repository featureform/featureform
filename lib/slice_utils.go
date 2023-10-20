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

func ToSet[A comparable](as []A) mapset.Set[A] {
	set := mapset.NewSet[A]()
	for _, item := range as {
		set.Add(item)
	}
	return set
}
