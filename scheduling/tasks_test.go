package scheduling

import (
	"testing"
	"time"
)

func TestSerializeTaskMetadata(t *testing.T) {
	testCases := []struct {
		name       string
		task       TaskMetadata
		targettype TargetType
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
			targettype: "provider",
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
			targettype: "name_variant",
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
			if deserializeTask.getTarget().Type() != currTest.targettype {
				t.Fatalf("Got target type: %v\n Expected:%v", deserializeTask.getTarget().Type(), currTest.targettype)
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

		{
			name: "NoTarget",
			task: TaskMetadata{
				ID:     1,
				Name:   "nv_task",
				Type:   ResourceCreation,
				Target: nil,
				Date:   time.Now(),
			},
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			serializeTask, err := currTest.task.ToJSON()
			if err != nil {
				return
			}

			deserializeTask := TaskMetadata{}
			err = deserializeTask.FromJSON(serializeTask)
			if err != nil {
				return
			}

			if TaskMetadataIsEqual(deserializeTask, currTest.task) {
				t.Fatalf("Expected target should be different from output target")
			}
		})
	}
}

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

func TestCorruptJsonData(t *testing.T) {
	testCases := []struct {
		name      string
		inputfile []byte
		errMsg    string
	}{
		{
			name: "InvalidJson",
			inputfile: []byte(`{"id"1, "name": "provider_task", "type": "Monitoring", "target": {"name": "
postgres", "target_type": "provider"}, "date": "2021-08-26T15:04:05Z"}`),
			errMsg: "invalid character '1' after object key:value pair",
		},
		{
			name:      "MissingName",
			inputfile: []byte(`{"id": 1, "type": "Monitoring", "target": {"name": "postgres", "target_type": "provider"}, "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "Missing field 'name'",
		},
		{
			name:      "MissingTarget",
			inputfile: []byte(`{"id": 1, "name": "no_target", "type": "Monitoring", "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "Missing field 'target'",
		},
		{
			name:      "InvalidTaskType",
			inputfile: []byte(`{"id": 1, "name": "no_target", "type": "DoesntExist", "target": {"name": "postgres", "target_type": "provider"}, "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "No such task type: 'DoesntExist'",
		},
		{
			name:      "InvalidTargetType",
			inputfile: []byte(`{"id": 1, "name": "no_target", "type": "HealthCheck", "target": {"name": "postgres", "target_type": "NoTarget"}, "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "No such target type: 'NoTarget'",
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			response := TaskMetadata{}
			err := response.FromJSON(currTest.inputfile)
			if err == nil {
				t.Fatalf(currTest.errMsg)
			}
		})
	}
}
