package ffsync

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/featureform/fferr"
)

type OrderedId interface {
	Equals(other OrderedId) bool
	Less(other OrderedId) bool
	String() string
	Value() interface{} // Value returns the underlying value of the ordered ID
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(data []byte) error
}

type Uint64OrderedId uint64

func (id *Uint64OrderedId) Equals(other OrderedId) bool {
	return id.Value() == other.Value()
}

func (id *Uint64OrderedId) Less(other OrderedId) bool {
	if otherID, ok := other.(*Uint64OrderedId); ok {
		return *id < *otherID
	}
	return false
}

func (id *Uint64OrderedId) String() string {
	return fmt.Sprint(id.Value())
}

func (id *Uint64OrderedId) Value() interface{} {
	return uint64(*id)
}

func (id *Uint64OrderedId) UnmarshalJSON(data []byte) error {
	var tmp uint64
	if err := json.Unmarshal(data, &tmp); err != nil {
		return fferr.NewInternalError(fmt.Errorf("failed to unmarshal uint64OrderedId: %v", err))
	}

	*id = Uint64OrderedId(tmp)
	return nil
}

func (id *Uint64OrderedId) MarshalJSON() ([]byte, error) {
	return json.Marshal(uint64(*id))
}

type OrderedIdGenerator interface {
	NextId(namespace string) (OrderedId, error)
}

func NewMemoryOrderedIdGenerator() OrderedIdGenerator {
	return &memoryIdGenerator{
		counters: make(map[string]*Uint64OrderedId),
		mu:       sync.Mutex{},
	}
}

type memoryIdGenerator struct {
	counters map[string]*Uint64OrderedId
	mu       sync.Mutex
}

func (m *memoryIdGenerator) NextId(namespace string) (OrderedId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Initialize the counter if it doesn't exist
	if _, ok := m.counters[namespace]; !ok {
		var initialId Uint64OrderedId = 0
		m.counters[namespace] = &initialId
	}

	// Atomically increment the counter and return the next ID
	nextId := atomic.AddUint64((*uint64)(m.counters[namespace]), 1)
	return (*Uint64OrderedId)(&nextId), nil
}
