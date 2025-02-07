// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package metadata

import (
	"reflect"
	"testing"
)

func TestCoordinatorScheduleJobSerialize(t *testing.T) {
	resID := ResourceID{Name: "test", Variant: "foo", Type: FEATURE}
	scheduleJob := &CoordinatorScheduleJob{
		Attempts: 0,
		Resource: resID,
		Schedule: "* * * * *",
	}
	serialized, err := scheduleJob.Serialize()
	if err != nil {
		t.Fatalf("Could not serialize schedule job")
	}
	copyScheduleJob := &CoordinatorScheduleJob{}
	if err := copyScheduleJob.Deserialize(serialized); err != nil {
		t.Fatalf("Could not deserialize schedule job")
	}
	if !reflect.DeepEqual(copyScheduleJob, scheduleJob) {
		t.Fatalf("Information changed on serialization and deserialization for schedule job")
	}
}

func TestGetJobKeys(t *testing.T) {
	resID := ResourceID{Name: "test", Variant: "foo", Type: FEATURE}
	expectedJobKey := "JOB__FEATURE__test__foo"
	expectedScheduleJobKey := "SCHEDULEJOB__FEATURE__test__foo"
	jobKey := GetJobKey(resID)
	scheduleJobKey := GetScheduleJobKey(resID)
	if jobKey != expectedJobKey {
		t.Fatalf("Could not generate correct job key")
	}
	if scheduleJobKey != expectedScheduleJobKey {
		t.Fatalf("Could not generate correct schedule job key")
	}
}
