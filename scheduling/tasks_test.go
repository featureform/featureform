// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package scheduling

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/featureform/ffsync"
)

func TestSerializeTaskMetadata(t *testing.T) {
	id1 := ffsync.Uint64OrderedId(1)
	testCases := []struct {
		name       string
		task       TaskMetadata
		targettype TargetType
	}{
		{
			name: "WithProviderTarget",
			task: TaskMetadata{
				ID:       TaskID(id1),
				Name:     "provider_task",
				TaskType: HealthCheck,
				Target: Provider{
					Name: "postgres",
				},
				TargetType:  ProviderTarget,
				DateCreated: time.Now().Truncate(0).UTC(),
			},
			targettype: ProviderTarget,
		},
		{
			name: "WithNameVariantTarget",
			task: TaskMetadata{
				ID:       TaskID(id1),
				Name:     "nv_task",
				TaskType: ResourceCreation,
				Target: NameVariant{
					Name:    "transaction",
					Variant: "default",
				},
				TargetType:  NameVariantTarget,
				DateCreated: time.Now().Truncate(0).UTC(),
			},
			targettype: NameVariantTarget,
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			serializedTask, err := currTest.task.Marshal()
			if err != nil {
				t.Fatalf("failed to serialize task metadata: %v", err)
			}

			deserializedTask := TaskMetadata{}
			if err := deserializedTask.Unmarshal(serializedTask); err != nil {
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
				ID:       TaskID(ffsync.Uint64OrderedId(1)),
				Name:     "nv_task",
				TaskType: ResourceCreation,
				Target: NameVariant{
					Name:    "transaction",
					Variant: "default",
				},
				DateCreated: time.Now(),
			},
		},

		{
			name: "NoTarget",
			task: TaskMetadata{
				ID:          TaskID(ffsync.Uint64OrderedId(1)),
				Name:        "nv_task",
				TaskType:    ResourceCreation,
				Target:      nil,
				DateCreated: time.Now(),
			},
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			serializedTask, err := currTest.task.Marshal()
			if err != nil {
				return
			}

			deserializedTask := TaskMetadata{}
			err = deserializedTask.Unmarshal(serializedTask)
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
		postgres", "targetType": "Provider"}, "dateCreated": "2021-08-26T15:04:05Z"}`),
			errMsg: "invalid character '1' after object key:value pair",
		},
		{
			name:      "MissingName",
			inputfile: []byte(`{"id": 1, "type": "Monitoring", "target": {"name": "postgres", "targetType": "Provider"}, "dateCreated": "2021-08-26T15:04:05Z"}`),
			errMsg:    "Missing field 'name'",
		},
		{
			name:      "MissingTarget",
			inputfile: []byte(`{"id": 1, "name": "no_target", "type": "Monitoring", "dateCreated": "2021-08-26T15:04:05Z"}`),
			errMsg:    "Missing field 'target'",
		},
		{
			name:      "InvalidTaskType",
			inputfile: []byte(`{"id": 1, "name": "no_target", "type": "DoesntExist", "target": {"name": "postgres", "targetType": "Provider"}, "dateCreated": "2021-08-26T15:04:05Z"}`),
			errMsg:    "No such task type: 'DoesntExist'",
		},
		{
			name:      "InvalidTargetType",
			inputfile: []byte(`{"id": 1, "name": "no_target", "type": "HealthCheck", "target": {"name": "postgres", "targetType": "NoTarget"}, "dateCreated": "2021-08-26T15:04:05Z"}`),
			errMsg:    "No such target type: 'NoTarget'",
		},
		{
			name:      "InvalidTarget",
			inputfile: []byte(`{"id": 1, "name": "no_target", "type": "HealthCheck", "target": ["name": "postgres", "targetType": "Provider"], "dateCreated": "2021-08-26T15:04:05Z"}`),
			errMsg:    "No such target type: 'NoTarget'",
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			response := TaskMetadata{}
			err := response.Unmarshal(currTest.inputfile)
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
			inputfile: []byte(`{"id": 1, "name": "no_target", "type": "Monitoring", "dateCreated": "2021-08-26T15:04:05Z"}`),
			errMsg:    "Missing field 'target'",
		},
		{
			name:      "InvalidTargetType",
			inputfile: []byte(`{"id": 1, "name": "wrong_target_type", "type": "HealthCheck", "target": {"name": "postgres", "targetType": "NoTarget"}, "dateCreated": "2021-08-26T15:04:05Z"}`),
			errMsg:    "No such target type: 'NoTarget'",
		},
		{
			name:      "InvalidTarget",
			inputfile: []byte(`{"id": 1, "name": "wrong_target", "type": "HealthCheck", "target": ["name": "postgres", "targetType": "Provider"], "dateCreated": "2021-08-26T15:04:05Z"}`),
			errMsg:    "Target must be an interface",
		},
		{
			name:      "MissingTaskTarget",
			inputfile: []byte(`{"id": 1, "name": "no_target_type", "type": "HealthCheck", "target": {"name": "postgres"}, "dateCreated": "2021-08-26T15:04:05Z"}`),
			errMsg:    "targetType is missing",
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			response := TaskMetadata{}
			err := response.Unmarshal(currTest.inputfile)
			if err == nil {
				t.Fatalf(currTest.errMsg)
			}
		})
	}
}

func TestTaskID(t *testing.T) {
	id1 := NewIntTaskID(1)
	id2 := NewIntTaskID(2)
	id3 := NewIntTaskID(1)

	taskMap := map[TaskID]int{}
	taskMap[id1] = 1
	taskMap[id2] = 2
	taskMap[id3] = 3
	fmt.Printf("%v\n", taskMap)
}

func Test_wrapTaskMetadataProto(t *testing.T) {
	id := ffsync.Uint64OrderedId(1)
	tests := []struct {
		name    string
		task    TaskMetadata
		wantErr bool
	}{
		{
			"Resource Creation Name Variant",
			TaskMetadata{
				ID:         TaskID(id),
				Name:       "Some Name",
				TaskType:   ResourceCreation,
				TargetType: NameVariantTarget,
				Target: NameVariant{
					Name:         "name",
					Variant:      "Variant",
					ResourceType: "FEATURE",
				},
				DateCreated: time.Now().UTC(),
			},
			false,
		},
		{
			"Provider",
			TaskMetadata{
				ID:         TaskID(id),
				Name:       "Some Name",
				TaskType:   HealthCheck,
				TargetType: ProviderTarget,
				Target: Provider{
					Name: "my provider",
				},
				DateCreated: time.Now().UTC(),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proto, err := tt.task.ToProto()
			if (err != nil) != tt.wantErr {
				t.Errorf("wrapTaskMetadataProto() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got, err := WrapProtoTaskMetadata(proto)
			if (err != nil) != tt.wantErr {
				t.Errorf("wrapProtoTaskMetadata() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(tt.task, got) {
				t.Errorf("wrapTaskMetadataProto() \ngiven = %#v \n  got = %#v", tt.task, got)
			}
		})
	}
}
