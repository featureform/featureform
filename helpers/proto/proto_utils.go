package proto

import (
	"errors"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"reflect"
	"regexp"
)

/*
DiffProtos returns a map of the differences between two protobuf messages.

The map is keyed by the field, and the value is a two-element
array containing the values of the field in the two messages.
*/
func DiffProtos(a, b proto.Message) (DiffMap, error) {
	diffReporter := DiffReporter{
		diffMap: make(DiffMap),
	}
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return DiffMap{}, errors.New("protobuf messages are of different types")
	}

	opts := cmp.Options{
		protocmp.Transform(),
		cmp.Reporter(&diffReporter),
	}

	cmp.Diff(a, b, opts)
	return diffReporter.Result(), nil
}

type DiffMap map[string][2]string

type DiffReporter struct {
	path    cmp.Path
	diffMap DiffMap
}

func (r *DiffReporter) PushStep(ps cmp.PathStep) {
	println(ps)
	r.path = append(r.path, ps)
}

func (r *DiffReporter) Report(rs cmp.Result) {
	if !rs.Equal() {
		vx, vy := r.path.Last().Values()
		field := r.path[len(r.path)-2].String()
		wordPattern := regexp.MustCompile(`\pL+`)
		extractedField := wordPattern.FindString(field)
		r.diffMap[extractedField] = [2]string{vx.String(), vy.String()}
	}
}

func (r *DiffReporter) PopStep() {
	r.path = r.path[:len(r.path)-1]
}

func (r *DiffReporter) Result() DiffMap {
	return r.diffMap
}
