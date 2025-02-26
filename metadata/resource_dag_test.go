// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package metadata

import (
	"context"
	"testing"

	d "github.com/featureform/lib/dag"
	pb "github.com/featureform/metadata/proto"
	"google.golang.org/protobuf/proto"

	"github.com/featureform/scheduling"
)

func TestToTaskDAG(t *testing.T) {
	testCases := []struct {
		name                  string
		createResourceDagFunc func() (ResourceDAG, error)
		expectedTaskDagFunc   func() (scheduling.TaskDAG, error)
	}{
		{
			name:                  "Single Task per Resource",
			createResourceDagFunc: createDAGMockTestCase1,
			expectedTaskDagFunc:   createTaskDAGTestCase1,
		},
		{
			name:                  "Multiple Tasks per Resource",
			createResourceDagFunc: createDAGMockTestCase2,
			expectedTaskDagFunc:   createTaskDAGTestCase2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dag, err := tc.createResourceDagFunc()
			if err != nil {
				t.Errorf("Failed to create resource dag: %v", err)
			}

			taskDag, err := dag.ToTaskDAG()
			if err != nil {
				t.Errorf("Failed to get TaskDag: %v", err)
			}

			expectedTaskDag, err := tc.expectedTaskDagFunc()
			if err != nil {
				t.Errorf("Failed to create TaskDag: %v", err)
			}

			if taskDag.Vertices() != expectedTaskDag.Vertices() {
				t.Errorf("Expected %v vertices, got %v", expectedTaskDag.Vertices(), taskDag.Vertices())
			}

			expectedTasksSorted := expectedTaskDag.SortedTasks()
			sortedTasks := taskDag.SortedTasks()
			for i, task := range sortedTasks {
				if task != expectedTasksSorted[i] {
					t.Errorf("Expected task %v, got %v", expectedTasksSorted, sortedTasks)
				}
			}
		})
	}
}

func createDAGMockTestCase1() (ResourceDAG, error) {
	/*
		Test Case 1:
				R1
			   /  \
			  R2   R3
			   \  /
				R4
			   /  \
			  R5  R6
			   \  /
				R7
	*/
	r1 := MockResource{[]int32{1}}

	r2 := MockResource{[]int32{2}}

	r3 := MockResource{[]int32{3}}

	r4 := MockResource{[]int32{4}}

	r5 := MockResource{[]int32{5}}

	r6 := MockResource{[]int32{6}}

	r7 := MockResource{[]int32{7}}

	genericDAG, err := d.NewGenericDAG()
	if err != nil {
		return ResourceDAG{}, err
	}
	dag := ResourceDAG{
		dag: genericDAG,
	}

	err = dag.AddDependency(&r1, &r2)
	if err != nil {
		return ResourceDAG{}, err
	}

	err = dag.AddDependency(&r1, &r3)
	if err != nil {
		return ResourceDAG{}, err
	}

	err = dag.AddDependency(&r2, &r4)
	if err != nil {
		return ResourceDAG{}, err
	}

	err = dag.AddDependency(&r3, &r4)
	if err != nil {
		return ResourceDAG{}, err
	}

	err = dag.AddDependency(&r4, &r5)
	if err != nil {
		return ResourceDAG{}, err
	}

	err = dag.AddDependency(&r4, &r6)
	if err != nil {
		return ResourceDAG{}, err
	}

	err = dag.AddDependency(&r5, &r7)
	if err != nil {
		return ResourceDAG{}, err
	}

	err = dag.AddDependency(&r6, &r7)
	if err != nil {
		return ResourceDAG{}, err
	}

	return dag, nil
}

func createTaskDAGTestCase1() (scheduling.TaskDAG, error) {
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
	task1 := scheduling.NewIntTaskID(1)
	task2 := scheduling.NewIntTaskID(2)
	task3 := scheduling.NewIntTaskID(3)
	task4 := scheduling.NewIntTaskID(4)
	task5 := scheduling.NewIntTaskID(5)
	task6 := scheduling.NewIntTaskID(6)
	task7 := scheduling.NewIntTaskID(7)

	dag, err := scheduling.NewTaskDAG()
	if err != nil {
		return scheduling.TaskDAG{}, err
	}

	err = dag.AddDependency(task1, task2)
	if err != nil {
		return scheduling.TaskDAG{}, err
	}

	err = dag.AddDependency(task1, task3)
	if err != nil {
		return scheduling.TaskDAG{}, err
	}

	err = dag.AddDependency(task2, task4)
	if err != nil {
		return scheduling.TaskDAG{}, err
	}

	err = dag.AddDependency(task3, task4)
	if err != nil {
		return scheduling.TaskDAG{}, err
	}

	err = dag.AddDependency(task4, task5)
	if err != nil {
		return scheduling.TaskDAG{}, err
	}

	err = dag.AddDependency(task4, task6)
	if err != nil {
		return scheduling.TaskDAG{}, err
	}

	err = dag.AddDependency(task5, task7)
	if err != nil {
		return scheduling.TaskDAG{}, err
	}

	err = dag.AddDependency(task6, task7)
	if err != nil {
		return scheduling.TaskDAG{}, err
	}

	return dag, nil
}

func createDAGMockTestCase2() (ResourceDAG, error) {
	/*
		Test Case 1:
				R1
			   /  \
			  R2   R3
	*/
	r1 := MockResource{[]int32{3, 4}}

	r2 := MockResource{[]int32{1}}

	r3 := MockResource{[]int32{2}}

	genericDAG, err := d.NewGenericDAG()
	if err != nil {
		return ResourceDAG{}, err
	}
	dag := ResourceDAG{
		dag: genericDAG,
	}

	err = dag.AddDependency(&r1, &r2)
	if err != nil {
		return ResourceDAG{}, err
	}

	err = dag.AddDependency(&r1, &r3)
	if err != nil {
		return ResourceDAG{}, err
	}

	return dag, nil
}

func createTaskDAGTestCase2() (scheduling.TaskDAG, error) {
	/*
		Test Case 1:
				T4
			    |
				T3
			   /  \
			  T1   T2
	*/
	task1 := scheduling.NewIntTaskID(1)
	task2 := scheduling.NewIntTaskID(2)
	task3 := scheduling.NewIntTaskID(3)
	task4 := scheduling.NewIntTaskID(4)

	dag, err := scheduling.NewTaskDAG()
	if err != nil {
		return scheduling.TaskDAG{}, err
	}

	err = dag.AddDependency(task4, task3)
	if err != nil {
		return scheduling.TaskDAG{}, err
	}

	err = dag.AddDependency(task3, task1)
	if err != nil {
		return scheduling.TaskDAG{}, err
	}

	err = dag.AddDependency(task3, task2)
	if err != nil {
		return scheduling.TaskDAG{}, err
	}

	return dag, nil
}

type MockResource struct {
	taskIDs []int32
}

func (m *MockResource) TaskID() []scheduling.TaskID {
	taskIDs := []scheduling.TaskID{}
	for _, id := range m.taskIDs {
		taskIDs = append(taskIDs, scheduling.NewIntTaskID(uint64(id)))
	}
	return taskIDs
}
func (m *MockResource) Notify(context.Context, ResourceLookup, operation, Resource) error {
	return nil
}
func (m *MockResource) ID() ResourceID {
	return ResourceID{}
}
func (m *MockResource) Equals(other any) bool {
	if _, ok := other.(*MockResource); !ok {
		return false
	}
	return m == other
}

func (m *MockResource) Less(other any) bool {
	if _, ok := other.(*MockResource); !ok {
		return false
	}
	return m.ID().String() < other.(Resource).ID().String()
}

func (m *MockResource) Schedule() string {
	return ""
}
func (m *MockResource) Dependencies(context.Context, ResourceLookup) (ResourceLookup, error) {
	return nil, nil
}
func (m *MockResource) Proto() proto.Message {
	return nil
}
func (m *MockResource) GetStatus() *pb.ResourceStatus {
	return nil
}
func (m *MockResource) UpdateStatus(*pb.ResourceStatus) error {
	return nil
}
func (m *MockResource) UpdateSchedule(string) error {
	return nil
}
func (m *MockResource) Update(ResourceLookup, Resource) error {
	return nil
}

func (m *MockResource) TaskIDs() ([]scheduling.TaskID, error) {
	var taskIDs []scheduling.TaskID
	for _, id := range m.taskIDs {
		taskIDs = append(taskIDs, scheduling.NewIntTaskID(uint64(id)))
	}
	return taskIDs, nil
}

func (m *MockResource) ToDashboardDoc() ResourceDashboardDoc {
	return ResourceDashboardDoc{}
}
