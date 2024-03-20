package ffsync

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type OrderedId interface {
	Equals(other OrderedId) bool
	Less(other OrderedId) bool
	String() string
}

type uint64OrderedId uint64

func (id uint64OrderedId) Equals(other OrderedId) bool {
	return id == other.(uint64OrderedId)
}

func (id uint64OrderedId) Less(other OrderedId) bool {
	return id < other.(uint64OrderedId)
}

func (id uint64OrderedId) String() string {
	return fmt.Sprint(uint64(id))
}

type OrderedIdGenerator interface {
	NextId(namespace string) (OrderedId, error)
}

func NewMemoryIdGenerator() OrderedIdGenerator {
	return &memoryIdGenerator{
		counters: make(map[string]*uint64OrderedId),
		mu:       sync.Mutex{},
	}
}

type memoryIdGenerator struct {
	counters map[string]*uint64OrderedId
	mu       sync.Mutex
}

func (m *memoryIdGenerator) NextId(namespace string) (OrderedId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Initialize the counter if it doesn't exist
	if _, ok := m.counters[namespace]; !ok {
		var initialId uint64OrderedId = 0
		m.counters[namespace] = &initialId
	}

	// Atomically increment the counter and return the next ID
	nextId := atomic.AddUint64((*uint64)(m.counters[namespace]), 1)
	return uint64OrderedId(nextId), nil
}
