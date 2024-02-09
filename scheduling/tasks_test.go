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
				ID:       1,
				Name:     "provider_task",
				TaskType: HealthCheck,
				Target: Provider{
					Name:       "postgres",
					TargetType: ProviderTarget,
				},
				Date: time.Now().Truncate(0),
			},
			targettype: "Provider",
		},
		{
			name: "WithNameVariantTarget",
			task: TaskMetadata{
				ID:       1,
				Name:     "nv_task",
				TaskType: ResourceCreation,
				Target: NameVariant{
					Name:       "transaction",
					TargetType: NameVariantTarget,
				},
				Date: time.Now().Truncate(0),
			},
			targettype: "nameVariant",
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
			if deserializedTask.Target.Type() != currTest.targettype {
				t.Fatalf("Got target type: %v\n Expected:%v", deserializedTask.Target.Type(), currTest.targettype)
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
				ID:       1,
				Name:     "nv_task",
				TaskType: ResourceCreation,
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
				ID:       1,
				Name:     "nv_task",
				TaskType: ResourceCreation,
				Target:   nil,
				Date:     time.Now(),
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

func TestCorruptJsonData(t *testing.T) {
	testCases := []struct {
		name      string
		inputfile []byte
		errMsg    string
	}{
		{
			name: "InvalidJson",
			inputfile: []byte(`{"id"1, "name": "provider_task", "type": "Monitoring", "target": {"name": "
		postgres", "targetType": "Provider"}, "date": "2021-08-26T15:04:05Z"}`),
			errMsg: "invalid character '1' after object key:value pair",
		},
		{
			name:      "MissingName",
			inputfile: []byte(`{"id": 1, "type": "Monitoring", "target": {"name": "postgres", "targetType": "Provider"}, "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "Missing field 'name'",
		},
		{
			name:      "MissingTarget",
			inputfile: []byte(`{"id": 1, "name": "no_target", "type": "Monitoring", "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "Missing field 'target'",
		},
		{
			name:      "InvalidTaskType",
			inputfile: []byte(`{"id": 1, "name": "no_target", "type": "DoesntExist", "target": {"name": "postgres", "targetType": "Provider"}, "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "No such task type: 'DoesntExist'",
		},
		{
			name:      "InvalidTargetType",
			inputfile: []byte(`{"id": 1, "name": "no_target", "type": "HealthCheck", "target": {"name": "postgres", "targetType": "NoTarget"}, "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "No such target type: 'NoTarget'",
		},
		{
			name:      "InvalidTarget",
			inputfile: []byte(`{"id": 1, "name": "no_target", "type": "HealthCheck", "target": ["name": "postgres", "targetType": "Provider"], "date": "2021-08-26T15:04:05Z"}`),
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
			inputfile: []byte(`{"id": 1, "name": "wrong_target_type", "type": "HealthCheck", "target": {"name": "postgres", "targetType": "NoTarget"}, "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "No such target type: 'NoTarget'",
		},
		{
			name:      "InvalidTarget",
			inputfile: []byte(`{"id": 1, "name": "wrong_target", "type": "HealthCheck", "target": ["name": "postgres", "targetType": "Provider"], "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "Target must be an interface",
		},
		{
			name:      "MissingTaskTarget",
			inputfile: []byte(`{"id": 1, "name": "no_target_type", "type": "HealthCheck", "target": {"name": "postgres"}, "date": "2021-08-26T15:04:05Z"}`),
			errMsg:    "targetType is missing",
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
