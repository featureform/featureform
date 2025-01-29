// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package scheduling

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/featureform/ffsync"
	"github.com/featureform/logging"
	"github.com/featureform/metadata/proto"
	ptypes "github.com/featureform/provider/types"
	ss "github.com/featureform/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

func TestGetEmptyTasksWithMemory(t *testing.T) {
	ctx := logging.NewTestContext(t)
	manager, err := NewMemoryTaskMetadataManager(ctx)
	if err != nil {
		t.Fatalf(err.Error())
	}

	_, err = manager.GetAllTasks()
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestTaskMetadataManager(t *testing.T) {
	testFns := map[string]func(*testing.T, context.Context, TaskMetadataManager){
		"CreateTask": testCreateTask,
	}

	ctx := logging.NewTestContext(t)
	memoryTaskMetadataManager, err := NewMemoryTaskMetadataManager(ctx)
	if err != nil {
		t.Fatalf("failed to create memory task metadata manager: %v", err)
	}

	for name, fn := range testFns {
		t.Run(name, func(t *testing.T) {
			fn(t, ctx, memoryTaskMetadataManager)
		})
	}
}

func testCreateTask(t *testing.T, ctx context.Context, manager TaskMetadataManager) {
	type taskInfo struct {
		Name       string
		Type       TaskType
		Target     TaskTarget
		ExpectedID TaskID
	}
	tests := []struct {
		Name        string
		Tasks       []taskInfo
		shouldError bool
	}{
		{
			"Single",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(ffsync.Uint64OrderedId(1))},
			},
			false,
		},
		{
			"Multiple",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(ffsync.Uint64OrderedId(1))},
				{"name2", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(ffsync.Uint64OrderedId(2))},
				{"name3", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(ffsync.Uint64OrderedId(3))},
			},
			false,
		},
	}

	fn := func(t *testing.T, tasks []taskInfo, shouldError bool) {
		ctx := logging.NewTestContext(t)
		manager, err := NewMemoryTaskMetadataManager(ctx) // TODO: will need to modify this to use any store and deletes tasks after job was done
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range tasks {
			taskDef, err := manager.CreateTask(ctx, task.Name, task.Type, task.Target)
			if err != nil && shouldError {
				continue
			} else if err != nil && !shouldError {
				t.Fatalf("failed to create task: %v", err)
			} else if err == nil && shouldError {
				t.Fatalf("expected error but did not receive one")
			}
			if !task.ExpectedID.Equals(taskDef.ID) {
				t.Fatalf("Expected id: %d, got: %d", task.ExpectedID, taskDef.ID)
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt.Tasks, tt.shouldError)
		})
	}
}

func TestTaskGetByID(t *testing.T) {
	type taskInfo struct {
		Name       string
		Type       TaskType
		Target     TaskTarget
		ExpectedID TaskID
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		ID          TaskID
		shouldError bool
	}
	tests := []TestCase{
		{
			"Empty",
			[]taskInfo{},
			TaskID(ffsync.Uint64OrderedId(1)),
			true,
		},
		{
			"Single",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(ffsync.Uint64OrderedId(1))},
			},
			TaskID(ffsync.Uint64OrderedId(1)),
			false,
		},
		{
			"Multiple",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(ffsync.Uint64OrderedId(1))},
				{"name2", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(ffsync.Uint64OrderedId(2))},
				{"name3", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(ffsync.Uint64OrderedId(3))},
			},
			TaskID(ffsync.Uint64OrderedId(2)),
			false,
		},
		{
			"MultipleInsertInvalidLookup",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(ffsync.Uint64OrderedId(1))},
				{"name2", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(ffsync.Uint64OrderedId(2))},
				{"name3", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(ffsync.Uint64OrderedId(3))},
			},
			TaskID(ffsync.Uint64OrderedId(4)),
			true,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		ctx := logging.NewTestContext(t)
		manager, err := NewMemoryTaskMetadataManager(ctx)
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(ctx, task.Name, task.Type, task.Target)
			if err != nil {
				t.Fatalf("failed to create task: %v", err)
			}
		}
		recievedDef, err := manager.GetTaskByID(test.ID)
		if err != nil && !test.shouldError {
			t.Fatalf("failed to fetch definiton: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error and did not get one")
		} else if err != nil && test.shouldError {
			return
		}

		idx := test.ID.(ffsync.Uint64OrderedId) - 1 // Ids are 1 indexed
		if reflect.DeepEqual(recievedDef, test.Tasks[idx]) {
			t.Fatalf("Expected: %v got: %v", test.Tasks[idx], recievedDef)
		}

	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestTaskGetAll(t *testing.T) {
	id1 := ffsync.Uint64OrderedId(1)
	id2 := ffsync.Uint64OrderedId(2)
	id3 := ffsync.Uint64OrderedId(3)

	type taskInfo struct {
		Name       string
		Type       TaskType
		Target     TaskTarget
		ExpectedID TaskID
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		ID          TaskID
		shouldError bool
	}
	tests := []TestCase{
		{
			"Empty",
			[]taskInfo{},
			TaskID(&id1),
			false,
		},
		{
			"Single",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&id2)},
			},
			TaskID(&id1),
			false,
		},
		{
			"Multiple",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&id1)},
				{"name2", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&id2)},
				{"name3", ResourceCreation, NameVariant{"name", "variant", "type"}, TaskID(&id3)},
			},
			TaskID(&id2),
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		ctx := logging.NewTestContext(t)
		manager, err := NewMemoryTaskMetadataManager(ctx)
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		var definitions []TaskMetadata
		for _, task := range test.Tasks {
			taskDef, err := manager.CreateTask(ctx, task.Name, task.Type, task.Target)
			if err != nil {
				t.Fatalf("failed to create task: %v", err)
			}
			definitions = append(definitions, taskDef)
		}
		recvTasks, err := manager.GetAllTasks()
		if err != nil && !test.shouldError {
			t.Fatalf("failed to fetch definiton: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error and did not get one")
		} else if err != nil && test.shouldError {
			return
		}

		if len(recvTasks) != len(test.Tasks) {
			t.Fatalf("Expected %d tasks, got %d tasks", len(test.Tasks), len(recvTasks))
		}

		for i, def := range definitions {
			foundDef := false
			for _, recvTask := range recvTasks {
				if reflect.DeepEqual(def, recvTask) {
					foundDef = true
					break
				}
			}
			if !foundDef {
				t.Fatalf("Expected %v, got: %v", def, recvTasks[i])
			}
		}

	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestCreateTaskRun(t *testing.T) {
	id1 := ffsync.Uint64OrderedId(1)
	id2 := ffsync.Uint64OrderedId(2)
	id3 := ffsync.Uint64OrderedId(3)
	id4 := ffsync.Uint64OrderedId(4)

	type taskInfo struct {
		Name   string
		Type   TaskType
		Target TaskTarget
	}
	type runInfo struct {
		Name       string
		TaskID     TaskID
		Trigger    Trigger
		ExpectedID TaskRunID
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		Runs        []runInfo
		shouldError bool
	}

	tests := []TestCase{
		{
			"Single",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(&id1), OnApplyTrigger{"name"}, TaskRunID(&id1)}},
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, TaskRunID(&id1)},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, TaskRunID(&id2)},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, TaskRunID(&id3)},
			},
			false,
		},
		{
			"InvalidTask",
			[]taskInfo{},
			[]runInfo{{"name", TaskID(&id1), OnApplyTrigger{"name"}, TaskRunID(&id1)}},
			true,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, TaskRunID(&id1)},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, TaskRunID(&id2)},
				{"name", TaskID(&id2), OnApplyTrigger{"name"}, TaskRunID(&id3)},
				{"name", TaskID(&id2), OnApplyTrigger{"name"}, TaskRunID(&id4)},
			},
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		ctx := logging.NewTestContext(t)
		manager, err := NewMemoryTaskMetadataManager(ctx)
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(ctx, task.Name, task.Type, task.Target)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			}
		}
		for _, run := range test.Runs {
			recvRun, err := manager.CreateTaskRun(ctx, run.Name, run.TaskID, run.Trigger)
			if err != nil && test.shouldError {
				continue
			} else if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			} else if err == nil && test.shouldError {
				t.Fatalf("expected error but did not receive one")
			}
			if run.ExpectedID.Value() != recvRun.ID.Value() {
				t.Fatalf("Expected id: %d, got: %d", run.ExpectedID.Value(), recvRun.ID.Value())
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestGetRunByID(t *testing.T) {
	id1 := ffsync.Uint64OrderedId(1)
	id2 := ffsync.Uint64OrderedId(2)
	id3 := ffsync.Uint64OrderedId(3)

	type taskInfo struct {
		Name   string
		Type   TaskType
		Target TaskTarget
	}
	type runInfo struct {
		Name    string
		TaskID  TaskID
		Trigger Trigger
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		Runs        []runInfo
		FetchTask   TaskID
		FetchRun    TaskRunID
		shouldError bool
	}
	tests := []TestCase{
		{
			"Single",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(id1), OnApplyTrigger{"name"}}},
			TaskID(id1),
			TaskRunID(id1),
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(id1), OnApplyTrigger{"name"}},
				{"name", TaskID(id1), OnApplyTrigger{"name"}},
				{"name", TaskID(id1), OnApplyTrigger{"name"}},
			},
			TaskID(id1),
			TaskRunID(id2),
			false,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(id1), OnApplyTrigger{"name"}},
				{"name", TaskID(id1), OnApplyTrigger{"name"}},
				{"name", TaskID(id2), OnApplyTrigger{"name"}},
				{"name", TaskID(id2), OnApplyTrigger{"name"}},
			},
			TaskID(id2),
			TaskRunID(id3),
			false,
		},
		{
			"Fetch NonExistent",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}}},
			TaskID(ffsync.Uint64OrderedId(1)),
			TaskRunID(ffsync.Uint64OrderedId(2)),
			true,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		ctx := logging.NewTestContext(t)
		manager, err := NewMemoryTaskMetadataManager(ctx)
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(ctx, task.Name, task.Type, task.Target)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			}
		}

		var runDefs []TaskRunMetadata
		for _, run := range test.Runs {
			runDef, err := manager.CreateTaskRun(ctx, run.Name, run.TaskID, run.Trigger)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task run: %v", err)
			}
			runDefs = append(runDefs, runDef)
		}
		recvRun, err := manager.GetRunByID(test.FetchTask, test.FetchRun)
		if err != nil && test.shouldError {
			return
		} else if err != nil && !test.shouldError {
			t.Fatalf("failed to get task by ID: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error but did not receive one")
		}
		for _, runDef := range runDefs {
			if runDef.TaskId == test.FetchTask && runDef.ID == test.FetchRun {
				if !reflect.DeepEqual(runDef, recvRun) {
					t.Fatalf("Expcted %v, got: %v", runDef, recvRun)
				}
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestGetRunAll(t *testing.T) {
	id1 := ffsync.Uint64OrderedId(1)
	id2 := ffsync.Uint64OrderedId(2)

	type taskInfo struct {
		Name   string
		Type   TaskType
		Target TaskTarget
	}
	type runInfo struct {
		Name    string
		TaskID  TaskID
		Trigger Trigger
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		Runs        []runInfo
		shouldError bool
	}
	tests := []TestCase{
		{
			"Single",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(id1), OnApplyTrigger{"name"}}},
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(id1), OnApplyTrigger{"name"}},
				{"name", TaskID(id1), OnApplyTrigger{"name"}},
				{"name", TaskID(id1), OnApplyTrigger{"name"}},
			},
			false,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(id1), OnApplyTrigger{"name"}},
				{"name", TaskID(id1), OnApplyTrigger{"name"}},
				{"name", TaskID(id2), OnApplyTrigger{"name"}},
				{"name", TaskID(id2), OnApplyTrigger{"name"}},
			},
			false,
		},
		{
			"Empty",
			[]taskInfo{},
			[]runInfo{},
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		ctx := logging.NewTestContext(t)
		manager, err := NewMemoryTaskMetadataManager(ctx)
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(ctx, task.Name, task.Type, task.Target)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			}
		}

		var runDefs TaskRunList
		for _, run := range test.Runs {
			runDef, err := manager.CreateTaskRun(ctx, run.Name, run.TaskID, run.Trigger)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task run: %v", err)
			}
			runDefs = append(runDefs, runDef)
		}
		recvRuns, err := manager.GetAllTaskRuns()
		if err != nil && test.shouldError {
			return
		} else if err != nil && !test.shouldError {
			t.Fatalf("failed to get task by ID: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error but did not receive one")
		}

		for _, runDef := range runDefs {
			foundDef := false
			for _, recvRun := range recvRuns {
				if reflect.DeepEqual(runDef, recvRun) {
					foundDef = true
					break
				}
			}
			if !foundDef {
				t.Fatalf("\nExpected %#v\n    got: %#v", runDef, recvRuns)
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestSetStatusByRunID(t *testing.T) {
	type taskInfo struct {
		Name   string
		Type   TaskType
		Target TaskTarget
	}
	type runInfo struct {
		Name    string
		TaskID  TaskID
		Trigger Trigger
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		Runs        []runInfo
		ForTask     TaskID
		ForRun      TaskRunID
		SetStatus   *proto.ResourceStatus
		shouldError bool
	}
	tests := []TestCase{
		{
			"Single",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}}},
			TaskID(ffsync.Uint64OrderedId(1)),
			TaskRunID(ffsync.Uint64OrderedId(1)),
			&proto.ResourceStatus{Status: proto.ResourceStatus_RUNNING},
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
			},
			TaskID(ffsync.Uint64OrderedId(1)),
			TaskRunID(ffsync.Uint64OrderedId(1)),
			&proto.ResourceStatus{Status: proto.ResourceStatus_RUNNING},
			false,
		},
		{
			"WrongID",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
			},
			TaskID(ffsync.Uint64OrderedId(1)),
			TaskRunID(ffsync.Uint64OrderedId(2)),
			&proto.ResourceStatus{Status: proto.ResourceStatus_RUNNING},
			true,
		},
		{
			"WrongRunID",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
			},
			TaskID(ffsync.Uint64OrderedId(1)),
			TaskRunID(ffsync.Uint64OrderedId(2)),
			&proto.ResourceStatus{Status: proto.ResourceStatus_PENDING},
			true,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(2)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(2)), OnApplyTrigger{"name"}},
			},
			TaskID(ffsync.Uint64OrderedId(2)),
			TaskRunID(ffsync.Uint64OrderedId(3)),
			&proto.ResourceStatus{Status: proto.ResourceStatus_RUNNING},
			false,
		},
		{
			"FailedStatusWithoutError",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
			},
			TaskID(ffsync.Uint64OrderedId(1)),
			TaskRunID(ffsync.Uint64OrderedId(1)),
			&proto.ResourceStatus{
				Status: proto.ResourceStatus_FAILED,
			},
			true,
		},
		{
			"FailedStatusWithStatusError",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
			},
			TaskID(ffsync.Uint64OrderedId(1)),
			TaskRunID(ffsync.Uint64OrderedId(1)),
			&proto.ResourceStatus{
				Status: proto.ResourceStatus_FAILED,
				ErrorStatus: &proto.ErrorStatus{
					Code:    0,
					Message: "some error",
				},
			},
			false,
		},
		{
			"FailedStatusWithErrorMessage",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
			},
			TaskID(ffsync.Uint64OrderedId(1)),
			TaskRunID(ffsync.Uint64OrderedId(1)),
			&proto.ResourceStatus{
				Status:       proto.ResourceStatus_FAILED,
				ErrorMessage: "some error",
			},
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		ctx := logging.NewTestContext(t)
		manager, err := NewMemoryTaskMetadataManager(ctx)
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(ctx, task.Name, task.Type, task.Target)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			}
		}

		for _, run := range test.Runs {
			_, err := manager.CreateTaskRun(ctx, run.Name, run.TaskID, run.Trigger)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task run: %v", err)
			}
		}

		// Set status to running before setting to failed
		if test.SetStatus.Status == proto.ResourceStatus_FAILED {
			err = manager.SetRunStatus(test.ForRun, test.ForTask, &proto.ResourceStatus{Status: proto.ResourceStatus_RUNNING})
			if err != nil {
				t.Fatalf("failed to set status correctly: %v", err)
			}
		}

		err = manager.SetRunStatus(test.ForRun, test.ForTask, test.SetStatus)
		if err != nil && test.shouldError {
			return
		} else if err != nil && !test.shouldError {
			t.Fatalf("failed to set status correctly: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error but did not receive one")
		}

		recvRun, err := manager.GetRunByID(test.ForTask, test.ForRun)
		if err != nil {
			t.Fatalf("failed to get run by ID %d: %v", test.ForTask, err)
		}

		recvStatus := recvRun.Status
		if recvStatus.Proto() != test.SetStatus.Status {
			t.Fatalf("Expcted %v, got: %v", test.SetStatus, recvStatus)
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestSetResumeID(t *testing.T) {
	type taskInfo struct {
		Name   string
		Type   TaskType
		Target TaskTarget
	}
	type runInfo struct {
		Name    string
		TaskID  TaskID
		Trigger Trigger
	}
	type TestCase struct {
		Name    string
		Tasks   []taskInfo
		Runs    []runInfo
		ForTask TaskID
		ForRun  TaskRunID
		SetID   ptypes.ResumeID
	}

	tests := []TestCase{
		{
			"Single",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}}},
			TaskID(ffsync.Uint64OrderedId(1)),
			TaskRunID(ffsync.Uint64OrderedId(1)),
			"ABC",
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
			},
			TaskID(ffsync.Uint64OrderedId(1)),
			TaskID(ffsync.Uint64OrderedId(2)),
			"DEF",
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(2)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(2)), OnApplyTrigger{"name"}},
			},
			TaskID(ffsync.Uint64OrderedId(2)),
			TaskRunID(ffsync.Uint64OrderedId(3)),
			"GHI",
		},
	}

	fn := func(t *testing.T, test TestCase) {
		ctx := logging.NewTestContext(t)
		manager, err := NewMemoryTaskMetadataManager(ctx)
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(ctx, task.Name, task.Type, task.Target)
			if err != nil {
				t.Fatalf("failed to create task: %v", err)
			}
		}

		for _, run := range test.Runs {
			_, err := manager.CreateTaskRun(ctx, run.Name, run.TaskID, run.Trigger)
			if err != nil {
				t.Fatalf("failed to create task run: %v", err)
			}
		}

		err = manager.SetResumeID(test.ForRun, test.ForTask, test.SetID)
		if err != nil {
			t.Fatalf("failed to set run ResumeID correctly: %v", err)
		}

		recvRun, err := manager.GetRunByID(test.ForTask, test.ForRun)
		if err != nil {
			t.Fatalf("failed to get run by ID %d: %v", test.ForTask, err)
		}

		if recvRun.ResumeID != test.SetID {
			t.Fatalf("expected %v, got: %v", test.SetID, recvRun.ResumeID)
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}
func TestSetEndTimeByRunID(t *testing.T) {
	type taskInfo struct {
		Name   string
		Type   TaskType
		Target TaskTarget
	}
	type runInfo struct {
		Name    string
		TaskID  TaskID
		Trigger Trigger
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		Runs        []runInfo
		ForTask     TaskID
		ForRun      TaskRunID
		SetTime     time.Time
		shouldError bool
	}

	tests := []TestCase{
		{
			"Single",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}}},
			TaskID(ffsync.Uint64OrderedId(1)),
			TaskRunID(ffsync.Uint64OrderedId(1)),
			time.Now().Add(3 * time.Minute).Truncate(0).UTC(),
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
			},
			TaskID(ffsync.Uint64OrderedId(1)),
			TaskID(ffsync.Uint64OrderedId(2)),
			time.Now().Add(3 * time.Minute).Truncate(0).UTC(),
			false,
		},
		{
			"EmptyTime",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
			},
			TaskID(ffsync.Uint64OrderedId(1)),
			TaskRunID(ffsync.Uint64OrderedId(2)),
			time.Time{},
			true,
		},
		{
			"WrongEndTime",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
			},
			TaskID(ffsync.Uint64OrderedId(1)),
			TaskRunID(ffsync.Uint64OrderedId(2)),
			time.Unix(1, 0).Truncate(0).UTC(),
			true,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(1)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(2)), OnApplyTrigger{"name"}},
				{"name", TaskID(ffsync.Uint64OrderedId(2)), OnApplyTrigger{"name"}},
			},
			TaskID(ffsync.Uint64OrderedId(2)),
			TaskRunID(ffsync.Uint64OrderedId(3)),
			time.Now().UTC().Add(3 * time.Minute).Truncate(0),
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		ctx := logging.NewTestContext(t)
		manager, err := NewMemoryTaskMetadataManager(ctx)
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(ctx, task.Name, task.Type, task.Target)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			}
		}

		for _, run := range test.Runs {
			_, err := manager.CreateTaskRun(ctx, run.Name, run.TaskID, run.Trigger)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task run: %v", err)
			}
		}

		err = manager.SetRunEndTime(test.ForRun, test.ForTask, test.SetTime)
		if err != nil && test.shouldError {
			return
		} else if err != nil && !test.shouldError {
			t.Fatalf("failed to set run end time correctly: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error but did not receive one")
		}

		recvRun, err := manager.GetRunByID(test.ForTask, test.ForRun)
		if err != nil {
			t.Fatalf("failed to get run by ID %d: %v", test.ForTask, err)
		}

		if recvRun.EndTime != test.SetTime {
			t.Fatalf("expected %v, got: %v", test.SetTime, recvRun.EndTime)
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestGetRunsByDate(t *testing.T) {
	id1 := ffsync.Uint64OrderedId(1)
	id2 := ffsync.Uint64OrderedId(2)
	id3 := ffsync.Uint64OrderedId(3)
	id4 := ffsync.Uint64OrderedId(4)

	type taskInfo struct {
		Name   string
		Type   TaskType
		Target TaskTarget
	}
	type runInfo struct {
		Name    string
		TaskID  TaskID
		Trigger Trigger
		Date    time.Time
	}
	type expectedRunInfo struct {
		TaskID TaskID
		RunID  TaskRunID
	}

	type TestCase struct {
		Name         string
		Tasks        []taskInfo
		Runs         []runInfo
		ExpectedRuns []expectedRunInfo
		ForTask      TaskID
		ForDate      time.Time
		shouldError  bool
	}

	tests := []TestCase{
		{
			"Empty",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{},
			[]expectedRunInfo{},
			TaskID(&id1),
			time.Now().UTC(),
			false,
		},
		{
			"Single",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(&id1), OnApplyTrigger{"name"}, time.Now().UTC().Add(3 * time.Minute).Truncate(0).UTC()}},
			[]expectedRunInfo{{TaskID(&id1), TaskRunID(&id1)}},
			TaskID(&id1),
			time.Now().UTC(),
			false,
		},
		{
			"Multiple",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, time.Now().UTC().Add(1 * time.Minute).Truncate(0).UTC()},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, time.Now().UTC().Add(2 * time.Minute).Truncate(0).UTC()},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, time.Now().UTC().Add(3 * time.Minute).Truncate(0).UTC()},
			},
			[]expectedRunInfo{
				{TaskID(&id1), TaskRunID(&id1)},
				{TaskID(&id1), TaskRunID(&id2)},
				{TaskID(&id1), TaskRunID(&id3)},
			},
			TaskID(&id1),
			time.Now().UTC(),
			false,
		},
		{
			"MultipleTasks",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}},
			},
			[]runInfo{
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, time.Now().
					Add(1 * time.Minute).Truncate(0).UTC()},
				{"name", TaskID(&id1), OnApplyTrigger{"name"}, time.Now().
					Add(2 * time.Minute).Truncate(0).UTC()},
				{"name", TaskID(&id2), OnApplyTrigger{"name"}, time.Now().
					Add(3 * time.Minute).Truncate(0).UTC()},
				{"name", TaskID(&id2), OnApplyTrigger{"name"}, time.Now().
					Add(4 * time.Minute).Truncate(0).UTC()},
			},
			[]expectedRunInfo{
				{TaskID(&id1), TaskRunID(&id1)},
				{TaskID(&id1), TaskRunID(&id2)},
				{TaskID(&id2), TaskRunID(&id3)},
				{TaskID(&id2), TaskRunID(&id4)},
			},
			TaskID(ffsync.Uint64OrderedId(2)),
			time.Now().UTC(),
			false,
		},
	}

	fn := func(t *testing.T, test TestCase) {
		ctx := logging.NewTestContext(t)
		manager, err := NewMemoryTaskMetadataManager(ctx)
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(ctx, task.Name, task.Type, task.Target)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			}
		}

		for _, run := range test.Runs {
			_, err := manager.CreateTaskRun(ctx, run.Name, run.TaskID, run.Trigger)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task run: %v", err)
			}
		}

		recvRuns, err := manager.GetRunsByDate(test.ForDate, test.ForDate.Add(24*time.Hour))
		if err != nil && test.shouldError {
			return
		} else if err != nil && !test.shouldError {
			t.Fatalf("failed to get runs by date: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error but did not receive one")
		}

		for _, run := range test.ExpectedRuns {
			foundRun := false
			for _, recvRun := range recvRuns {
				if recvRun.TaskId.Value() == run.TaskID.Value() && recvRun.ID.Value() == run.RunID.Value() {
					foundRun = true
					break
				}
			}
			if !foundRun {
				t.Fatalf("Expected Task %v, got: %v", run, recvRuns)
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestKeyPaths(t *testing.T) {
	type testCase struct {
		Name        string
		Key         Key
		ExpectedKey string
	}

	tests := []testCase{
		{
			Name:        "TaskMetadataKeyAll",
			Key:         TaskMetadataKey{},
			ExpectedKey: "/tasks/metadata/task_id=",
		},
		{
			Name:        "TaskMetadataKeyIndividual",
			Key:         TaskMetadataKey{taskID: TaskID(ffsync.Uint64OrderedId(1))},
			ExpectedKey: "/tasks/metadata/task_id=1",
		},
		{
			Name:        "TaskRunKeyAll",
			Key:         TaskRunKey{},
			ExpectedKey: "/tasks/runs/task_id=",
		},
		{
			Name:        "TaskRunKeyIndividual",
			Key:         TaskRunKey{taskID: TaskID(ffsync.Uint64OrderedId(1))},
			ExpectedKey: "/tasks/runs/task_id=1"},
		{
			Name:        "TaskRunMetadataKeyAll",
			Key:         TaskRunMetadataKey{},
			ExpectedKey: "/tasks/runs/metadata",
		},
		{
			Name: "TaskRunMetadataKeyIndividual",
			Key: TaskRunMetadataKey{
				taskID: TaskID(ffsync.Uint64OrderedId(1)),
				runID:  TaskRunID(ffsync.Uint64OrderedId(1)),
				date:   time.Date(2023, time.January, 20, 1, 1, 0, 0, time.UTC),
			},
			ExpectedKey: "/tasks/runs/metadata/2023/01/20/01/01/task_id=1/run_id=1",
		},
		{
			Name: "TaskRunMetadataKeyYearOnly",
			Key: TaskRunMetadataKey{
				date: time.Date(2023, time.January, 20, 2, 2, 0, 0, time.UTC),
			},
			ExpectedKey: "/tasks/runs/metadata/2023/01/20/02/02",
		},
	}

	fn := func(t *testing.T, test testCase) {
		if test.Key.String() != test.ExpectedKey {
			t.Fatalf("Expected: %s, got: %s", test.ExpectedKey, test.Key.String())
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestGetIncompleteRuns(t *testing.T) {
	id1 := ffsync.Uint64OrderedId(1)

	type taskInfo struct {
		Name   string
		Type   TaskType
		Target TaskTarget
	}
	type runInfo struct {
		Name      string
		TaskID    TaskID
		Trigger   Trigger
		SetStatus Status
	}
	type TestCase struct {
		Name        string
		Tasks       []taskInfo
		Runs        []runInfo
		NumResults  int
		shouldError bool
	}
	tests := []TestCase{
		{
			"All Pending",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{{"name", TaskID(id1), OnApplyTrigger{"name"}, PENDING}},
			1,
			false,
		},
		{
			"One Running",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(id1), OnApplyTrigger{"name"}, PENDING},
				{"name", TaskID(id1), OnApplyTrigger{"name"}, RUNNING},
			},
			2,
			false,
		},
		{
			"Two Complete",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(id1), OnApplyTrigger{"name"}, PENDING},
				{"name", TaskID(id1), OnApplyTrigger{"name"}, RUNNING},
				{"name", TaskID(id1), OnApplyTrigger{"name"}, FAILED},
				{"name", TaskID(id1), OnApplyTrigger{"name"}, READY},
			},
			2,
			false,
		},
		{
			"All Complete",
			[]taskInfo{{"name", ResourceCreation, NameVariant{"name", "variant", "type"}}},
			[]runInfo{
				{"name", TaskID(id1), OnApplyTrigger{"name"}, FAILED},
				{"name", TaskID(id1), OnApplyTrigger{"name"}, READY},
			},
			0,
			false,
		},
	}

	setStatus := func(t *testing.T, manager TaskMetadataManager, run TaskRunMetadata, status Status) TaskRunMetadata {
		if status == PENDING {
			return run
		} else if status == RUNNING {
			err := manager.SetRunStatus(run.ID, run.TaskId, &proto.ResourceStatus{Status: proto.ResourceStatus_RUNNING})
			if err != nil {
				t.Fatalf("failed to set run status: %v", err)
			}
		} else if status == FAILED {
			err := manager.SetRunStatus(run.ID, run.TaskId, &proto.ResourceStatus{Status: proto.ResourceStatus_RUNNING})
			if err != nil {
				t.Fatalf("failed to set run status: %v", err)
			}
			err = manager.SetRunStatus(run.ID, run.TaskId, &proto.ResourceStatus{Status: proto.ResourceStatus_FAILED, ErrorMessage: "failed"})
			if err != nil {
				t.Fatalf("failed to set run status: %v", err)
			}
		} else if status == READY {
			err := manager.SetRunStatus(run.ID, run.TaskId, &proto.ResourceStatus{Status: proto.ResourceStatus_RUNNING})
			if err != nil {
				t.Fatalf("failed to set run status: %v", err)
			}
			err = manager.SetRunStatus(run.ID, run.TaskId, &proto.ResourceStatus{Status: proto.ResourceStatus_READY})
			if err != nil {
				t.Fatalf("failed to set run status: %v", err)
			}
		}
		updatedRun, err := manager.GetRunByID(run.TaskId, run.ID)
		if err != nil {
			t.Fatalf("failed to get run by id: %v", err)
		}
		return updatedRun
	}

	fn := func(t *testing.T, test TestCase) {
		ctx := logging.NewTestContext(t)
		manager, err := NewMemoryTaskMetadataManager(ctx)
		if err != nil {
			t.Fatalf("failed to create memory task metadata manager: %v", err)
		}

		for _, task := range test.Tasks {
			_, err := manager.CreateTask(ctx, task.Name, task.Type, task.Target)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task: %v", err)
			}
		}

		var runDefs TaskRunList
		for _, run := range test.Runs {
			runDef, err := manager.CreateTaskRun(ctx, run.Name, run.TaskID, run.Trigger)
			if err != nil && !test.shouldError {
				t.Fatalf("failed to create task run: %v", err)
			}
			runDef = setStatus(t, manager, runDef, run.SetStatus)
			runDefs = append(runDefs, runDef)
		}

		recvRuns, err := manager.GetUnfinishedTaskRuns()
		if err != nil && test.shouldError {
			return
		} else if err != nil && !test.shouldError {
			t.Fatalf("failed to get task by ID: %v", err)
		} else if err == nil && test.shouldError {
			t.Fatalf("expected error but did not receive one")
		}

		runDefs = runDefs.FilterByStatus(PENDING, RUNNING)

		if len(recvRuns) != test.NumResults {
			t.Fatalf("\nExpected %d\n    got: %d", test.NumResults, len(recvRuns))
		}

		for _, runDef := range runDefs {
			foundDef := false
			for _, recvRun := range recvRuns {
				if reflect.DeepEqual(runDef, recvRun) {
					foundDef = true
					break
				}
			}
			if !foundDef {
				t.Fatalf("\nExpected %#v\n    got: %#v", runDef, recvRuns)
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			fn(t, tt)
		})
	}
}

func TestTaskRunList_FilterByStatus(t *testing.T) {
	type args struct {
		statuses []Status
	}
	tests := []struct {
		name string
		trl  TaskRunList
		args args
		want TaskRunList
	}{
		{"Empty", TaskRunList{TaskRunMetadata{Status: CREATED}}, args{[]Status{}}, TaskRunList{}},
		{"No Status Provided", TaskRunList{TaskRunMetadata{Status: CREATED}, TaskRunMetadata{Status: RUNNING}}, args{[]Status{}}, TaskRunList{}},
		{"One Status", TaskRunList{TaskRunMetadata{Status: CREATED}, TaskRunMetadata{Status: RUNNING}, TaskRunMetadata{Status: PENDING}}, args{[]Status{PENDING}}, TaskRunList{TaskRunMetadata{Status: PENDING}}},
		{"Two Statuses", TaskRunList{TaskRunMetadata{Status: CREATED}, TaskRunMetadata{Status: RUNNING}, TaskRunMetadata{Status: PENDING}}, args{[]Status{PENDING, RUNNING}}, TaskRunList{TaskRunMetadata{Status: RUNNING}, TaskRunMetadata{Status: PENDING}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.trl.FilterByStatus(tt.args.statuses...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FilterByStatus() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

type MockGenerator struct {
	counter uint64
}

func (m *MockGenerator) NextId(ctx context.Context, namespace string) (ffsync.OrderedId, error) {
	m.counter++ // increment so future calls give a new value
	return ffsync.Uint64OrderedId(uint64(m.counter)), nil
}

func (m MockGenerator) Close() {
}

type MockNotifier struct {
	mockerCalled    bool
	resourceType    string
	resourceName    string
	resourceVariant string
	status          string
	errorMesssage   string
	mutx            sync.Mutex
	wg              *sync.WaitGroup
}

func (m *MockNotifier) ChangeNotification(resourceTypeParam, resourceNameParam, resourceVariantParam, statusParam, errorMessageParam string) error {
	defer m.wg.Done() //makes sure the change notif is done and returns in the test
	m.mutx.Lock()
	defer m.mutx.Unlock()
	//capture the passed in params to verify in the test itself
	m.mockerCalled = true
	m.resourceType = resourceTypeParam
	m.resourceName = resourceNameParam
	m.resourceVariant = resourceVariantParam
	m.status = statusParam
	m.errorMesssage = errorMessageParam
	return nil
}

func (m *MockNotifier) ErrorNotification(resource, error string) error {
	return nil
}

func Test_SetRunStatus_Invokes_Notification(t *testing.T) {
	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}
	memoryStorage, err := ss.NewMemoryStorageImplementation()
	if err != nil {
		t.Fatalf(err.Error())
	}
	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	storage := ss.MetadataStorage{
		Locker:          &locker,
		Storage:         &memoryStorage,
		SkipListLocking: true,
		Logger:          logger,
	}

	mockGen := MockGenerator{}
	var wg sync.WaitGroup
	wg.Add(1)
	mockNotif := MockNotifier{
		wg: &wg,
	}

	taskManager := TaskMetadataManager{
		Storage:     storage,
		idGenerator: &mockGen,
		notifier:    &mockNotif,
	}

	taskRunId := TaskRunID(ffsync.Uint64OrderedId(1))
	taskId := TaskID(ffsync.Uint64OrderedId(1))

	//sets up the data layer for record retrieval
	runErr := memoryStorage.Set("/tasks/runs/task_id=1", `{"taskID":1,"runs":[{"runID":1,"dateCreated":"2024-10-15T23:10:20.709277839Z"}]}`)
	if runErr != nil {
		t.Fatalf("Failed to set run: %v", runErr)
	}

	taskErr := memoryStorage.Set("/tasks/metadata/task_id=1", `{"id":1,"name":"mytask","type":0,"target":{"name":"transactions-test",`+
		`"variant":"2024-10-15t18-10-18","type":"SOURCE_VARIANT"},"targetType":0,"dateCreated":"2024-10-15T23:10:20.709277839Z"}`)
	if taskErr != nil {
		t.Fatalf("Failed to set task: %v", taskErr)
	}

	metaErr := memoryStorage.Set("/tasks/runs/metadata/2024/10/15/23/10/task_id=1/run_id=1", `{"runId":1,"taskId":1, `+
		`"name":"Create Resource transactions-test (2024-10-15t18-10-18)","trigger":{"triggerName":"Apply"},`+
		`"triggerType":1,"target":{"name":"transactions-test","variant":"2024-10-15t18-10-18","type":"SOURCE_VARIANT"},`+
		`"targetType":0,"dependencies":null,"status":2,"startTime":"2024-10-15T23:10:20.709277839Z","endTime":"0001-01-01T00:00:00Z",`+
		`"logs":["Starting Test...", "Ending Test"],"error":"","dag":{},"lastSuccessful":null,"resumeID":"","ErrorProto":null}`)
	if metaErr != nil {
		t.Fatalf("Failed to set task/run metadata: %v", metaErr)
	}

	// will set the resource from PENDING state to RUNNING, fails on error
	updateErr := taskManager.SetRunStatus(taskRunId, taskId, &proto.ResourceStatus{Status: proto.ResourceStatus_RUNNING})
	if updateErr != nil {
		assert.FailNow(t, fmt.Sprintf("SetRunStatus() resulted in a runtime error of %v", updateErr))
	}

	// once clear, wait for the notif to fire
	// verify the update fires AND with the correct data
	wg.Wait()
	assert.True(t, mockNotif.mockerCalled, "ChangeNotification was not called!")
	assert.Equal(t, "transactions-test", mockNotif.resourceName)
	assert.Equal(t, "SOURCE_VARIANT", mockNotif.resourceType)
	assert.Equal(t, "2024-10-15t18-10-18", mockNotif.resourceVariant)
	assert.Equal(t, proto.ResourceStatus_RUNNING.String(), mockNotif.status)
	assert.Empty(t, mockNotif.errorMesssage)
}

func Test_notifyChange(t *testing.T) {

	testNameVariant := NameVariant{
		Name:         "test name",
		Variant:      "test variant",
		ResourceType: "test type",
	}

	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	storage := ss.MetadataStorage{
		Logger: logger,
	}

	testCases := []struct {
		name         string
		shouldNotify bool
		hasNotifier  bool
		target       NameVariant
		targetType   TargetType
		error        error
	}{
		{
			"fire notification since all conditions are met",
			true,
			true,
			testNameVariant,
			NameVariantTarget,
			nil,
		},
		{
			"no notification if the notifier is NIL",
			false,
			false,
			testNameVariant,
			NameVariantTarget,
			nil,
		},
		{
			"no notification if target type is PROVIDER",
			false,
			true,
			testNameVariant,
			ProviderTarget,
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			wg.Add(1)

			mockNotif := MockNotifier{wg: &wg}
			taskManager := TaskMetadataManager{
				Storage:  storage,
				notifier: &mockNotif,
			}

			if !tt.hasNotifier {
				taskManager.notifier = nil
			}

			updatedMetadata := TaskRunMetadata{
				TargetType: tt.targetType,
				Target:     tt.target,
				Status:     Status(proto.ResourceStatus_READY),
			}

			taskManager.notifyChange(updatedMetadata, tt.error)

			done := make(chan bool)
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done: //the notification goroutine completed OK
				if tt.shouldNotify {
					assert.True(t, mockNotif.mockerCalled, "Expected ChangeNotification to be called.")
					assert.Equal(t, tt.target.Name, mockNotif.resourceName)
					assert.Equal(t, tt.target.ResourceType, mockNotif.resourceType)
					assert.Equal(t, tt.target.Variant, mockNotif.resourceVariant)
					assert.Equal(t, proto.ResourceStatus_READY.String(), mockNotif.status)
				} else {
					assert.False(t, mockNotif.mockerCalled, "Expected Changenotification to NOT be called.")
				}
			case <-time.After(100 * time.Millisecond): //the test go routine hangs
				if tt.shouldNotify {
					assert.FailNow(t, "Test timed out while waiting for a notification")
				}
				//in the case it shouldn't, verify the mocker was not called at all
				assert.False(t, mockNotif.mockerCalled, "Expected Changenotification to NOT be called")
			}
		})
	}
}
