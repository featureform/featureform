package scheduling

import (
	"testing"
	"time"
)

func TestSerializeTaskMetadata(t *testing.T) {
	testCases := []struct {
		name string
		task TaskMetadata
	}{
		{
			name: "WithProviderTarget",
			task: TaskMetadata{
				ID:   1,
				Name: "provider_task",
				Type: HealthCheck,
				Target: Provider{
					Name:       "postgres",
					TargetType: ProviderTarget,
				},
				Date: time.Now(),
			},
		},
		{
			name: "WithNameVariantTarget",
			task: TaskMetadata{
				ID:   1,
				Name: "nv_task",
				Type: ResourceCreation,
				Target: NameVariant{
					Name:       "transaction",
					TargetType: NameVariantTarget,
				},
				Date: time.Now(),
			},
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			serializeTask, err := currTest.task.ToJSON()
			if err != nil {
				t.Errorf("failed to serialize task metadata: %v", err)
			}

			deserializeTask := TaskMetadata{}
			if err := deserializeTask.FromJSON(serializeTask); err != nil {
				t.Errorf("failed to deserialize task metadata: %v", err)
			}

			if !TaskMetadataIsEqual(deserializeTask, currTest.task) {
				t.Fatalf("Wrong struct values: %v\nExpected: %v", deserializeTask, currTest.task)
			}
		})
	}
}

func TaskMetadataIsEqual(output, expected TaskMetadata) bool {
	return output.ID == expected.ID &&
		output.Name == expected.Name &&
		output.Type == expected.Type &&
		output.Target == expected.Target &&
		output.Date.Truncate(0) == expected.Date.Truncate(0)
}

func TestIncorrectTaskMetadata(t *testing.T) {
	testCases := []struct {
		name string
		task TaskMetadata
	}{
		{
			name: "NameVariantProviderTarget",
			task: TaskMetadata{
				ID:   1,
				Name: "nv_task",
				Type: ResourceCreation,
				Target: NameVariant{
					Name:       "transaction",
					TargetType: ProviderTarget,
				},
				Date: time.Now(),
			},
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			serializeTask, err := currTest.task.ToJSON()
			if err != nil {
				t.Errorf("failed to serialize task metadata: %v", err)
			}

			deserializeTask := TaskMetadata{}
			if err := deserializeTask.FromJSON(serializeTask); err != nil {
				t.Errorf("failed to deserialize task metadata: %v", err)
			}

			if TaskMetadataIsEqual(deserializeTask, currTest.task) {
				t.Fatalf("Expected target should be different from output target")
			}
		})
	}
}

// Write a test to verify getID, getName, getTarget, and DateCreated methods
// of TaskMetadata struct.
func TestTaskMetadataGetMethods(t *testing.T) {
	testCases := []struct {
		name string
		task TaskMetadata
	}{
		{
			name: "TestGetMethods",
			task: TaskMetadata{
				ID:   1,
				Name: "provider_task",
				Type: Monitoring,
				Target: Provider{
					Name:       "postgres",
					TargetType: ProviderTarget,
				},
				Date: time.Now(),
			},
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			if currTest.task.getID() != currTest.task.ID {
				t.Fatalf("Expected ID should be equal to output ID")
			}

			if currTest.task.getName() != currTest.task.Name {
				t.Fatalf("Expected Name should be equal to output Name")
			}

			if currTest.task.getTarget() != currTest.task.Target {
				t.Fatalf("Expected Target should be equal to output Target")
			}

			if currTest.task.DateCreated() != currTest.task.Date {
				t.Fatalf("Expected Date should be equal to output Date")
			}
		})
	}
}
