package scheduling

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestTriggerName(t *testing.T) {
	testCases := []struct {
		name     string
		trigger  Trigger
		expected string
	}{
		{
			name:     "OneOffTriggerName",
			trigger:  OneOffTrigger{TriggerName: "name1"},
			expected: "name1",
		},
		{
			name:     "DummyTriggerName",
			trigger:  DummyTrigger{TriggerName: "name2", DummyField: true},
			expected: "name2",
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			if currTest.trigger.Name() != currTest.expected {
				t.Fatalf("Got trigger name: %v\n Expected:%v", currTest.trigger.Name(), currTest.expected)
			}
		})
	}

}

func TestEmptyVariables(t *testing.T) {
	testCases := []struct {
		name   string
		task   TaskRunMetadata
		errMsg error
	}{
		{
			name: "NoName",
			task: TaskRunMetadata{
				ID:     1,
				TaskId: 12,
				Name:   "",
				Trigger: OneOffTrigger{
					TriggerName: "name1",
				},
				TriggerType: oneOffTrigger,
				Status:      Pending,
				StartTime:   time.Now().Truncate(0).UTC(),
				EndTime:     time.Now().Truncate(0).UTC(),
				Logs:        nil,
				Error:       "No name present",
			},
			errMsg: fmt.Errorf("task run metadata is missing name"),
		},
		{
			name: "NoStartTime",
			task: TaskRunMetadata{
				ID:     1,
				TaskId: 12,
				Name:   "name2",
				Trigger: OneOffTrigger{
					TriggerName: "name3",
				},
				TriggerType: oneOffTrigger,
				Status:      Pending,
				EndTime:     time.Now().Truncate(0).UTC(),
				Logs:        nil,
				Error:       "No start time present",
			},
			errMsg: fmt.Errorf("task run metadata is missing Start Time"),
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			serializedTask, err := currTest.task.Marshal()
			if err != nil {
				return
			}

			deserializedTask := TaskRunMetadata{}
			err = deserializedTask.Unmarshal(serializedTask)
			if err == nil {
				t.Fatalf("Expected error for empty fields")
			}
			if err.Error() != currTest.errMsg.Error() {
				t.Fatalf("Expected error: %v\n Got: %v", currTest.errMsg, err)
			}
		})
	}

}

func TestSerializeTaskRunMetadata(t *testing.T) {
	testCases := []struct {
		name        string
		task        TaskRunMetadata
		triggerType TriggerType
	}{
		{
			name: "WithOneOffTrigger",
			task: TaskRunMetadata{
				ID:     1,
				TaskId: 12,
				Name:   "oneoff_taskrun",
				Trigger: OneOffTrigger{
					TriggerName: "name1",
				},
				TriggerType: oneOffTrigger,
				Status:      Pending,
				StartTime:   time.Now().Truncate(0).UTC(),
				EndTime:     time.Now().Truncate(0).UTC(),
				Logs:        nil,
				Error:       "",
			},
			triggerType: "OneOffTrigger",
		},
		{
			name: "WithDummyTrigger",
			task: TaskRunMetadata{
				ID:     1,
				TaskId: 12,
				Name:   "dummy_taskrun",
				Trigger: DummyTrigger{
					TriggerName: "name2",
					DummyField:  true,
				},
				TriggerType: dummyTrigger,
				Status:      Failed,
				StartTime:   time.Now().Truncate(0).UTC(),
				EndTime:     time.Now().Truncate(0).UTC(),
				Logs:        nil,
				Error:       "",
			},
			triggerType: "DummyTrigger",
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			serializedTask, err := currTest.task.Marshal()
			if err != nil {
				t.Fatalf("failed to serialize task run metadata: %v", err)
			}

			deserializedTask := TaskRunMetadata{}
			if err := deserializedTask.Unmarshal(serializedTask); err != nil {
				t.Fatalf("failed to deserialize task run metadata: %v", err)
			}

			if !reflect.DeepEqual(deserializedTask, currTest.task) {
				t.Fatalf("Wrong struct values: %v\nExpected: %v", deserializedTask, currTest.task)
			}
			if deserializedTask.Trigger.Type() != currTest.triggerType {
				t.Fatalf("Got trigger type: %v\n Expected:%v", deserializedTask.Trigger.Type(), currTest.triggerType)
			}
		})
	}
}

func TestIncorrectTaskRunMetadata(t *testing.T) {
	testCases := []struct {
		name string
		task TaskRunMetadata
	}{
		{
			name: "OneOffDummyTrigger",
			task: TaskRunMetadata{
				ID:     1,
				TaskId: 12,
				Name:   "dummy_and_oneoff",
				Trigger: DummyTrigger{
					TriggerName: "name3",
					DummyField:  false,
				},
				TriggerType: oneOffTrigger,
				Status:      Failed,
				StartTime:   time.Now().Truncate(0).UTC(),
				EndTime:     time.Now().Truncate(0).UTC(),
				Logs:        nil,
				Error:       "Mixed trigger present",
			},
		},

		{
			name: "NoTrigger",
			task: TaskRunMetadata{
				ID:        1,
				TaskId:    12,
				Name:      "no_trigger",
				Trigger:   nil,
				Status:    Pending,
				StartTime: time.Now().Truncate(0).UTC(),
				EndTime:   time.Now().Truncate(0).UTC(),
				Logs:      nil,
				Error:     "No trigger present",
			},
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			serializedTask, err := currTest.task.Marshal()
			if err != nil {
				return
			}

			deserializedTask := TaskRunMetadata{}
			err = deserializedTask.Unmarshal(serializedTask)
			if err != nil {
				return
			}

			if reflect.DeepEqual(deserializedTask, currTest.task) {
				t.Fatalf("Expected trigger should be present and different from output trigger")
			}
		})
	}
}

func TestCorruptData(t *testing.T) {
	testCases := []struct {
		name      string
		inputfile []byte
		errMsg    string
	}{
		{
			name: "InvalidJson",
			inputfile: []byte(`{"id"1, "TaskID": 12, "name":"invalid_json_file",
			 "trigger": {"triggerName": "name4", "triggerType": "OneOffTrigger", "dummyField":  false,},
			  "status": "FAILED", "startTime": "2021-08-26T15:04:05Z", "endTime": "2021-08-26T15:04:05Z",
			   "logs": nil, "error": "invalid json",
			}`),
			errMsg: "invalid character '1' after object key:value pair",
		},
		{
			name: "MissingName",
			inputfile: []byte(`{"id":1, "TaskID": 12,
			"trigger": {"triggerName": "name5", "triggerType": "OneOffTrigger", "dummyField":  false,},
			 "status": "FAILED", "startTime": "2021-08-26T15:04:05Z", "endTime": "2021-08-26T15:04:05Z",
			  "logs": nil, "error": "invalid json",
		   }`),
			errMsg: "Missing field 'name'",
		},
		{
			name: "MissingTrigger",
			inputfile: []byte(`{"id":1, "TaskID": 12, "name":"invalid_json_file",
			 "status": "FAILED", "startTime": "2021-08-26T15:04:05Z", "endTime": "2021-08-26T15:04:05Z",
			  "logs": nil, "error": "invalid json",
		   }`),
			errMsg: "Missing field 'trigger'",
		},
		{
			name: "InvalidStatusType",
			inputfile: []byte(`{"id":1, "TaskID": 12, "name":"invalid_json_file",
			"trigger": {"triggerName": "name6", "triggerType": "OneOffTrigger", "dummyField":  false,},
			 "status": "NOSTATUS", "startTime": "2021-08-26T15:04:05Z", "endTime": "2021-08-26T15:04:05Z",
			  "logs": nil, "error": "invalid json",
		   }`),
			errMsg: "No such status: 'NOSTATUS'",
		},
		{
			name: "InvalidTriggerType",
			inputfile: []byte(`{"id":1, "TaskID": 12, "name":"invalid_json_file",
			"trigger": {"triggerName": "name7", "triggerType": "wrongTrigger", "dummyField":  false,},
			 "status": "PENDING", "startTime": "2021-08-26T15:04:05Z", "endTime": "2021-08-26T15:04:05Z",
			  "logs": nil, "error": "invalid json",
		   }`),
			errMsg: "No such trigger type: 'wrongTrigger'",
		},
		{
			name: "InvalidTrigger",
			inputfile: []byte(`{"id":1, "TaskID": 12, "name":"invalid_json_file",
			"trigger": ["triggerName": "name8", "triggerType": "DummyTrigger", "dummyField":  false],
			 "status": "PENDING", "startTime": "2021-08-26T15:04:05Z", "endTime": "2021-08-26T15:04:05Z",
			  "logs": nil, "error": "invalid json",
		   }`),
			errMsg: "Wrong format of Trigger",
		},
	}

	for _, currTest := range testCases {
		t.Run(currTest.name, func(t *testing.T) {
			response := TaskRunMetadata{}
			err := response.Unmarshal(currTest.inputfile)
			if err == nil {
				t.Fatalf(currTest.errMsg)
			}
		})
	}
}
