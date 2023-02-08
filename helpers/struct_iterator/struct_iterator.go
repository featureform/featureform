package struct_iterator

import (
	"fmt"
	"reflect"
	"unicode"
)

type StructIterator struct {
	idx      int
	numField int
	val      reflect.Value
}

func (si *StructIterator) Next() bool {
	si.idx += 1
	if si.inRange() && si.isPublic() {
		return true
	} else if si.inRange() && !si.isPublic() {
		return si.Next()
	}
	return false
}

func (si *StructIterator) inRange() bool {
	return si.idx < si.numField
}

func (si *StructIterator) ItemName() string {
	return si.val.Type().Field(si.idx).Name
}

func (si *StructIterator) ItemValue() interface{} {
	return si.val.Field(si.idx).Interface()
}

func (si *StructIterator) isPublic() bool {
	firstLetter := []rune(si.ItemName())[0]
	if unicode.IsLower(firstLetter) && unicode.IsLetter(firstLetter) {
		return false
	}
	return true
}

// NewStructIterator creates returns a new iterator for a struct.
// This only works with struct types, not maps. It will only iterate
// over public members of the struct.
func NewStructIterator(s interface{}) (*StructIterator, error) {
	t := reflect.TypeOf(s).Kind()
	if t != reflect.Struct {
		return nil, fmt.Errorf("cannot create iterator from type %T", s)
	}

	v := reflect.ValueOf(s)
	return &StructIterator{
		idx:      -1,
		numField: v.NumField(),
		val:      v,
	}, nil
}
