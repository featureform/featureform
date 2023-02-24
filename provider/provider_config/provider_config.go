package provider_config

import (
	"reflect"

	ss "github.com/featureform/helpers/string_set"
	si "github.com/featureform/helpers/struct_iterator"
)

type SerializedConfig []byte

func differingFields(a, b interface{}) (ss.StringSet, error) {
	diff := ss.StringSet{}
	aIter, err := si.NewStructIterator(a)
	if err != nil {
		return nil, err
	}

	bv := reflect.ValueOf(b)

	for aIter.Next() {
		key := aIter.Key()
		aVal := aIter.Value()
		bVal := bv.FieldByName(key).Interface()
		if !reflect.DeepEqual(aVal, bVal) {
			diff[key] = true
		}
	}

	return diff, nil
}
