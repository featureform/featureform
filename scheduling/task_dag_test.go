// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package scheduling

import (
	"fmt"
	"testing"

	"github.com/featureform/fferr"
)

func TestSetTaskRunID(t *testing.T) {
	dag, err := createDAGTestCase1()
	if err != nil {
		t.Errorf("Error creating DAG: %v", err)
	}

	task1 := NewIntTaskID(1)
	task10 := NewIntTaskID(10)

	taskRun1 := NewIntTaskRunID(1)
	taskRun2 := NewIntTaskRunID(2)

	err = dag.SetTaskRunID(task1, taskRun1)
	if err != nil {
		t.Errorf("Error setting task run ID: %v", err)
	}
	runID, err := dag.GetTaskRunID(task1)
	if err != nil {
		t.Errorf("Error getting task run ID: %v", err)
	}

	if runID != taskRun1 {
		t.Errorf("Expected %v, got %v", taskRun1, runID)
	}

	expectedAlreadyExistsError := fferr.NewInternalError(fmt.Errorf("task %v already has a run", task1))
	err = dag.SetTaskRunID(task1, taskRun2)
	if err != nil && err.Error() != expectedAlreadyExistsError.Error() {
		t.Errorf("Expected error %v, got %v", expectedAlreadyExistsError, err)
	} else if err == nil {
		t.Errorf("Expected error, got nil")
	}

	expectedNotInDAGError := fferr.NewInternalError(fmt.Errorf("task %v not in DAG", task10))
	err = dag.SetTaskRunID(task10, taskRun1)
	if err != nil && err.Error() != expectedNotInDAGError.Error() {
		t.Errorf("Expected error %v, got %v", expectedNotInDAGError, err)
	} else if err == nil {
		t.Errorf("Expected error, got nil")
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
		dag      TaskDAG
		expected []TaskID
	}{
		{
			name: "DAG1",
			dag:  dag1,
			expected: []TaskID{
				NewIntTaskID(7),
				NewIntTaskID(5),
				NewIntTaskID(6),
				NewIntTaskID(4),
				NewIntTaskID(2),
				NewIntTaskID(3),
				NewIntTaskID(1),
			},
		},
		{
			name: "DAG2",
			dag:  dag2,
			expected: []TaskID{
				NewIntTaskID(7),
				NewIntTaskID(6),
				NewIntTaskID(5),
				NewIntTaskID(4),
				NewIntTaskID(2),
				NewIntTaskID(3),
				NewIntTaskID(1),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sortedTasks := tc.dag.SortedTasks()
			if len(sortedTasks) != len(tc.expected) {
				t.Errorf("Expected %d tasks, got %d", len(tc.expected), len(sortedTasks))
			}

			for i, task := range sortedTasks {
				if task != tc.expected[i] {
					t.Errorf("Expected %v, got %v", tc.expected[i], task)
				}
			}
		})
	}
}

func createDAGTestCase1() (TaskDAG, error) {
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

	dag, err := NewTaskDAG()
	if err != nil {
		return TaskDAG{}, err
	}

	err = dag.AddDependency(NewIntTaskID(1), NewIntTaskID(2))
	if err != nil {
		return TaskDAG{}, err
	}
	err = dag.AddDependency(NewIntTaskID(1), NewIntTaskID(3))
	if err != nil {
		return TaskDAG{}, err
	}
	err = dag.AddDependency(NewIntTaskID(2), NewIntTaskID(4))
	if err != nil {
		return TaskDAG{}, err
	}
	err = dag.AddDependency(NewIntTaskID(3), NewIntTaskID(4))
	if err != nil {
		return TaskDAG{}, err
	}
	err = dag.AddDependency(NewIntTaskID(4), NewIntTaskID(5))
	if err != nil {
		return TaskDAG{}, err
	}
	err = dag.AddDependency(NewIntTaskID(4), NewIntTaskID(6))
	if err != nil {
		return TaskDAG{}, err
	}
	err = dag.AddDependency(NewIntTaskID(5), NewIntTaskID(7))
	if err != nil {
		return TaskDAG{}, err
	}
	err = dag.AddDependency(NewIntTaskID(6), NewIntTaskID(7))
	if err != nil {
		return TaskDAG{}, err
	}

	return dag, nil
}

func createDAGTestCase2() (TaskDAG, error) {
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
	dag, err := NewTaskDAG()
	if err != nil {
		return TaskDAG{}, err
	}

	err = dag.AddDependency(NewIntTaskID(1), NewIntTaskID(2))
	if err != nil {
		return TaskDAG{}, err
	}
	err = dag.AddDependency(NewIntTaskID(1), NewIntTaskID(3))
	if err != nil {
		return TaskDAG{}, err
	}
	err = dag.AddDependency(NewIntTaskID(2), NewIntTaskID(4))
	if err != nil {
		return TaskDAG{}, err
	}
	err = dag.AddDependency(NewIntTaskID(3), NewIntTaskID(4))
	if err != nil {
		return TaskDAG{}, err
	}
	err = dag.AddDependency(NewIntTaskID(4), NewIntTaskID(5))
	if err != nil {
		return TaskDAG{}, err
	}
	err = dag.AddDependency(NewIntTaskID(5), NewIntTaskID(6))
	if err != nil {
		return TaskDAG{}, err
	}
	err = dag.AddDependency(NewIntTaskID(6), NewIntTaskID(7))
	if err != nil {
		return TaskDAG{}, err
	}

	return dag, nil
}
