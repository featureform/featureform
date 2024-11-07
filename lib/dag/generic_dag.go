// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package dag

import (
	"fmt"
	"sort"

	"github.com/featureform/fferr"
)

// Node is used so we can compare nodes and utilize them in maps
type Node interface {
	Equals(any) bool
	// Less is used to keep consistent order when having multiple nodes
	// topographically sorted at the same elevation
	Less(any) bool
}

func NewGenericDAG() (GenericDAG, error) {
	return GenericDAG{
		adjList: make(map[Node][]Node),
	}, nil
}

type GenericDAG struct {
	vertices int
	adjList  map[Node][]Node
}

func (dag *GenericDAG) AddNode(node Node) {
	if _, exists := dag.adjList[node]; !exists {
		dag.adjList[node] = []Node{}
		dag.vertices++
	}
}

func (dag *GenericDAG) AddEdge(parent Node, child Node) error {
	// NodeODO: detect cycle when adding edge can be wasteful,
	// we can try to detect it by building a builder pattern.
	// We will have a GenericDAGBuilder that will have a method
	// AddEdge that will return a GenericDAGBuilder. Nodehe GenericDAGBuilder
	// will have a method Build that will return a GenericDAG which will
	// be immutable
	if parent == child || dag.detectCycle(parent, child) {
		return fferr.NewInternalError(fmt.Errorf("cycle detected in NodeaskDAG: %v -> %v", parent, child))
	}

	if _, exists := dag.adjList[parent]; !exists {
		dag.AddNode(parent)
	}
	if _, exists := dag.adjList[child]; !exists {
		dag.AddNode(child)
	}

	dag.adjList[parent] = append(dag.adjList[parent], child)

	return nil
}

func (dag *GenericDAG) detectCycle(parent Node, child Node) bool {
	visited := make(map[Node]bool)
	return dag.dfsVisit(child, parent, visited)
}

// dfsVisit does a Depth-first search to detect cycles
func (dag *GenericDAG) dfsVisit(current Node, target Node, visited map[Node]bool) bool {
	if visited[current] {
		return false // Already visited
	}
	if current == target {
		return true // Cycle detected
	}

	visited[current] = true
	for _, dep := range dag.adjList[current] {
		if dag.dfsVisit(dep, target, visited) {
			return true
		}
	}
	return false
}

// SortedNodes returns a topological sort of dependencies in the DAG
func (dag *GenericDAG) SortedNodes() []Node {
	visited := make(map[Node]bool)
	stack := make([]Node, 0)

	var tasks []Node
	for task := range dag.adjList {
		tasks = append(tasks, task)
	}

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Less(tasks[j])
	})

	for _, task := range tasks {
		if !visited[task] {
			dag.dfsSort(task, visited, &stack)
		}
	}
	return stack
}

// dfsSort does a Depth-first search to topologically sort the tasks
func (dag *GenericDAG) dfsSort(task Node, visited map[Node]bool, stack *[]Node) {
	visited[task] = true
	for _, dep := range dag.adjList[task] {
		if !visited[dep] {
			dag.dfsSort(dep, visited, stack)
		}
	}
	*stack = append(*stack, task)
}

// NodeExists Returns true if the node exists in the DAG
func (dag *GenericDAG) NodeExists(node Node) bool {
	_, exists := dag.adjList[node]
	return exists
}

func (dag *GenericDAG) GetNodeEdges(node Node) []Node {
	return dag.adjList[node]
}

// HasEdge Returns true if there is an edge from parent to child
func (dag *GenericDAG) HasEdge(parent Node, child Node) bool {
	for _, dep := range dag.adjList[parent] {
		if dep == child {
			return true
		}
	}
	return false
}

// Vertices Returns the number of vertices in the DAG
func (dag *GenericDAG) Vertices() int {
	return dag.vertices
}

func (dag *GenericDAG) GetEdgesAndNode() map[Node][]Node {
	return dag.adjList
}
