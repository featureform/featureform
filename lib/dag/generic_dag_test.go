// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package dag

import (
	"testing"
)

func TestGenericDAGCreation(t *testing.T) {
	testCase := []struct {
		name             string
		createDagFunc    func() (GenericDAG, error)
		expectedVertices int
		err              bool
	}{
		{
			name:             "Test Case 1",
			createDagFunc:    createDAGTestCase1,
			expectedVertices: 7,
			err:              false,
		},
		{
			name:             "Test Case 2",
			createDagFunc:    createDAGTestCase2,
			expectedVertices: 7,
			err:              false,
		},
		{
			name:             "Cycle Test Case 1",
			createDagFunc:    createCycleDAG1,
			expectedVertices: 0,
			err:              true,
		},
		{
			name:             "Cycle Test Case 2",
			createDagFunc:    createCycleDAG2,
			expectedVertices: 0,
			err:              true,
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			dag, err := tc.createDagFunc()
			if tc.err {
				if err == nil {
					t.Errorf("Expected error, got nil")
				}

			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				if tc.expectedVertices != dag.vertices {
					t.Errorf("Expected %v vertices, got %v", tc.expectedVertices, dag.vertices)
				}
			}
		})
	}
}

func TestSortedTasks(t *testing.T) {
	dag1, err := createDAGTestCase1()
	if err != nil {
		t.Errorf("Error creating DAG: %v", err)
	}

	dag2, err := createDAGTestCase2()
	if err != nil {
		t.Errorf("Error creating DAG: %v", err)
	}

	testCases := []struct {
		name     string
		dag      GenericDAG
		expected []NodeInt
	}{
		{
			name: "DAG1",
			dag:  dag1,
			expected: []NodeInt{
				7,
				5,
				6,
				4,
				2,
				3,
				1,
			},
		},
		{
			name: "DAG2",
			dag:  dag2,
			expected: []NodeInt{
				7,
				6,
				5,
				4,
				2,
				3,
				1,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sortedNodes := tc.dag.SortedNodes()
			if len(sortedNodes) != len(tc.expected) {
				t.Errorf("Expected %d tasks, got %d", len(tc.expected), len(sortedNodes))
			}

			for i, node := range sortedNodes {
				if node != tc.expected[i] {
					t.Errorf("Expected %v, got %v", tc.expected[i], node)
				}
			}
		})
	}
}

func TestNodeExists(t *testing.T) {
	dag, err := createDAGTestCase1()
	if err != nil {
		t.Errorf("Error creating DAG: %v", err)
	}

	testCases := []struct {
		name     string
		node     NodeInt
		expected bool
	}{
		{
			name:     "Node Exists",
			node:     1,
			expected: true,
		},
		{
			name:     "Node Does Not Exist",
			node:     8,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exists := dag.NodeExists(tc.node)
			if exists != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, exists)
			}
		})
	}
}

func TestHasEdge(t *testing.T) {
	dag, err := createDAGTestCase1()
	if err != nil {
		t.Errorf("Error creating DAG: %v", err)
	}

	testCases := []struct {
		name     string
		parent   NodeInt
		child    NodeInt
		expected bool
	}{
		{
			name:     "Edge Exists",
			parent:   1,
			child:    2,
			expected: true,
		},
		{
			name:     "Edge Does Not Exist",
			parent:   2,
			child:    1,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exists := dag.HasEdge(tc.parent, tc.child)
			if exists != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, exists)
			}
		})
	}
}

func createDAGTestCase1() (GenericDAG, error) {
	/*
		Test Case 1:
				T1
			   /  \
			  T2   T3
			   \  /
				T4
			   /  \
			  T5  T6
			   \  /
				T7
	*/

	dag, err := NewGenericDAG()
	if err != nil {
		return GenericDAG{}, err
	}

	err = dag.AddEdge(NodeInt(1), NodeInt(2))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(1), NodeInt(3))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(2), NodeInt(4))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(3), NodeInt(4))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(4), NodeInt(5))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(4), NodeInt(6))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(5), NodeInt(7))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(6), NodeInt(7))
	if err != nil {
		return GenericDAG{}, err
	}

	return dag, nil
}

func createDAGTestCase2() (GenericDAG, error) {
	/*
		Test Case:
				T1
			   /  \
			  T2   T3
			   \  /
				T4
				|
				T5
				|
				T6
				|
				T7
	*/
	dag, err := NewGenericDAG()
	if err != nil {
		return GenericDAG{}, err
	}

	err = dag.AddEdge(NodeInt(1), NodeInt(2))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(1), NodeInt(3))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(2), NodeInt(4))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(3), NodeInt(4))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(4), NodeInt(5))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(5), NodeInt(6))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(6), NodeInt(7))
	if err != nil {
		return GenericDAG{}, err
	}

	return dag, nil
}

func createCycleDAG1() (GenericDAG, error) {
	/*
		Test
				T1
			   /  \
			  T2   T3
			   \  /
				T1
		Expected Failure
	*/
	task1 := 1
	task2 := 2
	task3 := 3

	dag, err := NewGenericDAG()
	if err != nil {
		return GenericDAG{}, err
	}

	err = dag.AddEdge(NodeInt(task1), NodeInt(task2))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(task1), NodeInt(task3))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(task2), NodeInt(task1))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(task3), NodeInt(task1))
	if err != nil {
		return GenericDAG{}, err
	}
	return dag, nil
}

func createCycleDAG2() (GenericDAG, error) {
	/*
		Test
				T1
			    |
			    T2
				|
				T3
				|
				T1
		Expected Failure
	*/
	task1 := 1
	task2 := 2
	task3 := 3

	dag, err := NewGenericDAG()
	if err != nil {
		return GenericDAG{}, err
	}

	err = dag.AddEdge(NodeInt(task1), NodeInt(task2))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(task2), NodeInt(task3))
	if err != nil {
		return GenericDAG{}, err
	}
	err = dag.AddEdge(NodeInt(task3), NodeInt(task1))
	if err != nil {
		return GenericDAG{}, err
	}
	return dag, nil
}

type NodeInt int

func (n NodeInt) Equals(other any) bool {
	return n == other.(NodeInt)
}

func (n NodeInt) Less(other any) bool {
	return n < other.(NodeInt)
}
