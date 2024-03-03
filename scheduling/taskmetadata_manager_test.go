package scheduling

import (
	"testing"
)

func TestTaskMetadataManager(t *testing.T) {
	testFns := map[string]func(*testing.T, TaskMetadataManager){
		"CreateTask": testCreateTask,
		// "GetTaskMetadata":  TaskMetadataGet,
		// "ListTaskMetadata": TaskMetadataList,
	}

	memoryTaskMetadataManager := NewMemoryTaskMetadataManager()

	for name, fn := range testFns {
		t.Run(name, func(t *testing.T) {
			fn(t, memoryTaskMetadataManager)
		})
	}
}

func testCreateTask(t *testing.T, manager TaskMetadataManager) {
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
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, 1},
			},
			false,
		},
		{
			"Multiple",
			[]taskInfo{
				{"name", ResourceCreation, NameVariant{"name", "variant", "type"}, 1},
				{"name2", ResourceCreation, NameVariant{"name", "variant", "type"}, 2},
				{"name3", ResourceCreation, NameVariant{"name", "variant", "type"}, 3},
			},
			false,
		},
	}

	fn := func(t *testing.T, tasks []taskInfo, shouldError bool) {
		manager := NewMemoryTaskMetadataManager() // TODO: will need to modify this to use any store and deletes tasks after job was done
		for _, task := range tasks {
			taskDef, err := manager.CreateTask(task.Name, task.Type, task.Target)
			if err != nil && shouldError {
				continue
			} else if err != nil && !shouldError {
				t.Fatalf("failed to create task: %v", err)
			} else if err == nil && shouldError {
				t.Fatalf("expected error but did not receive one")
			}
			if task.ExpectedID != taskDef.ID {
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
