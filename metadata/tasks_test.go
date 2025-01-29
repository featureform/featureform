// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package metadata

import (
	"testing"

	"github.com/featureform/logging"
	ptypes "github.com/featureform/provider/types"
	s "github.com/featureform/scheduling"
)

func TestSetRunID(t *testing.T) {
	ctx, logger := logging.NewTestContextAndLogger(t)
	serv, addr := startServ(t, ctx, logger)
	defer serv.GracefulStop()
	client := client(t, ctx, logger, addr)
	taskClient := client.Tasks

	target := s.NameVariant{Name: "a", Variant: "b", ResourceType: "test"}
	task, err := serv.taskManager.CreateTask(ctx, "mytask", s.ResourceCreation, target)
	if err != nil {
		t.Fatalf("Failed to create task: %s", err)
	}
	taskID := task.ID
	runID, err := taskClient.CreateRun("test", taskID, s.OnApplyTrigger{TriggerName: "Test"})
	if err != nil {
		t.Fatalf("Failed to create run ID: %s", err)
	}
	if taskMeta, err := taskClient.GetRun(taskID, runID); err != nil {
		t.Fatalf("Failed to create run ID: %s", err)
	} else if taskMeta.ResumeID != ptypes.NilResumeID {
		t.Fatalf("TaskMetadata instantiated without a nil Resume ID: %+v", taskMeta)
	}
	resumeID := ptypes.ResumeID("resumer")
	if err := taskClient.SetRunResumeID(taskID, runID, resumeID); err != nil {
		t.Fatalf("Failed to set resume ID: %s", err)
	}
	if taskMeta, err := taskClient.GetRun(taskID, runID); err != nil {
		t.Fatalf("Failed to create run ID: %s", err)
	} else if taskMeta.ResumeID != resumeID {
		t.Fatalf("Returned ResumeID did not match. Found: %s Expected: %s\nFull: %+v", taskMeta.ResumeID, resumeID, taskMeta)
	}

}
