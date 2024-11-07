// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package tasks

func NewNoopTaskFactory(task BaseTask) (Task, error) {
	return &NoopTask{BaseTask: task}, nil
}

type NoopTask struct {
	BaseTask
}

func (t *NoopTask) Run() error {
	return nil
}
