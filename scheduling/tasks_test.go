package scheduling

import (
	"reflect"
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
				id:       1,
				name:     "provider_task",
				taskType: HealthCheck,
				target: Provider{
					name:       "postgres",
					targetType: ProviderTarget,
				},
				date: time.Now().Truncate(0),
			},
			targettype: "provider",
		},
		{
			name: "WithNameVariantTarget",
			task: TaskMetadata{
				id:       1,
				name:     "nv_task",
				taskType: ResourceCreation,
				target: NameVariant{
					name:       "transaction",
					targetType: NameVariantTarget,
				},
				date: time.Now().Truncate(0),
			},
			targettype: "name_variant",
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			serializedTask, err := currTest.task.ToJSON()
			if err != nil {
				t.Fatalf("failed to serialize task metadata: %v", err)
			}

			deserializedTask := TaskMetadata{}
			if err := deserializedTask.FromJSON(serializedTask); err != nil {
				t.Fatalf("failed to deserialize task metadata: %v", err)
			}

			if !reflect.DeepEqual(deserializedTask, currTest.task) {
				t.Fatalf("Wrong struct values: %v\nExpected: %v", deserializedTask, currTest.task)
			}
			if deserializedTask.Target().Type() != currTest.targettype {
				t.Fatalf("Got target type: %v\n Expected:%v", deserializedTask.Target().Type(), currTest.targettype)
			}
		})
	}
}

func TestIncorrectTaskMetadata(t *testing.T) {
	testCases := []struct {
		name string
		task TaskMetadata
	}{
		{
			name: "NameVariantProviderTarget",
			task: TaskMetadata{
				id:       1,
				name:     "nv_task",
				taskType: ResourceCreation,
				target: NameVariant{
					name:       "transaction",
					targetType: ProviderTarget,
				},
				date: time.Now(),
			},
		},

		{
			name: "NoTarget",
			task: TaskMetadata{
				id:       1,
				name:     "nv_task",
				taskType: ResourceCreation,
				target:   nil,
				date:     time.Now(),
			},
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			serializedTask, err := currTest.task.ToJSON()
			if err != nil {
				return
			}

			deserializedTask := TaskMetadata{}
			err = deserializedTask.FromJSON(serializedTask)
			if err != nil {
				return
			}

			if reflect.DeepEqual(deserializedTask, currTest.task) {
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
				id:       1,
				name:     "provider_task",
				taskType: Monitoring,
				target: Provider{
					name:       "postgres",
					targetType: ProviderTarget,
				},
				date: time.Now(),
			},
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			if currTest.task.ID() != currTest.task.id {
				t.Fatalf("Expected ID should be equal to output ID")
			}

			if currTest.task.Name() != currTest.task.name {
				t.Fatalf("Expected Name should be equal to output Name")
			}

			if currTest.task.Target() != currTest.task.target {
				t.Fatalf("Expected Target should be equal to output Target")
			}

			if currTest.task.DateCreated() != currTest.task.date {
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
		{
			name:      "InvalidTarget",
			inputfile: []byte(`{"id": 1, "name": "no_target", "type": "HealthCheck", "target": ["name": "postgres", "target_type": "provider"], "date": "2021-08-26T15:04:05Z"}`),
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

func TestTarget(t *testing.T) {
	testCases := []struct {
		name      string
		inputfile []byte
		errMsg    string
	}{
		{
			name:      "MissingTarget",
			inputfile: []byte(`{"id": 1, "name": "no_target", "type": "Monitoring", "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "Missing field 'target'",
		},
		{
			name:      "InvalidTargetType",
			inputfile: []byte(`{"id": 1, "name": "wrong_target_type", "type": "HealthCheck", "target": {"name": "postgres", "target_type": "NoTarget"}, "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "No such target type: 'NoTarget'",
		},
		{
			name:      "InvalidTarget",
			inputfile: []byte(`{"id": 1, "name": "wrong_target", "type": "HealthCheck", "target": ["name": "postgres", "target_type": "provider"], "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "Target must be an interface",
		},
		{
			name:      "MissingTaskTarget",
			inputfile: []byte(`{"id": 1, "name": "no_target_type", "type": "HealthCheck", "target": {"name": "postgres"}, "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "target_type is missing",
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
