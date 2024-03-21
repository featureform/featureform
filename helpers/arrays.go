package helpers

import (
	"fmt"
	"reflect"
)

func RemoveItemsFromList[T any](slice []T, items []T) ([]T, error) {
	for _, item := range items {
		var err error
		slice, err = RemoveFromList(slice, item)
		if err != nil {
			return nil, err
		}
	}
	return slice, nil
}

func RemoveFromList[T any](slice []T, val T) ([]T, error) {
	index := -1
	// Find the index of the value in the slice
	for i, v := range slice {
		if reflect.DeepEqual(v, val) {
			index = i
			break
		}
	}
	// If the value is found, remove it using slicing
	if index != -1 {
		slice = append(slice[:index], slice[index+1:]...)
	} else {
		return nil, fmt.Errorf("resource not found in list: %v", val)
	}
	return slice, nil
}

func Contains(slice []int32, val int32) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}
