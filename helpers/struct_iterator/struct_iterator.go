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

func (si *StructIterator) Key() string {
	return si.val.Type().Field(si.idx).Name
}

func (si *StructIterator) Value() interface{} {
	return si.val.Field(si.idx).Interface()
}

// Tag Fetches the struct tag for the current key. If the
// specified tag does not exist, returns nil
func (si *StructIterator) Tag(tagKey string) string {
	tag := si.val.Type().Field(si.idx).Tag
	if alias, ok := tag.Lookup(tagKey); ok {
		return alias
	} else {
		return ""
	}
}

func (si *StructIterator) isPublic() bool {
	firstLetter := []rune(si.Key())[0]
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
