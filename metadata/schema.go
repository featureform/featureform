// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package metadata

import (
	s "github.com/featureform/scheduling"
	"github.com/featureform/schema"
)

const latestExecutorTaskRunLockPath schema.Version = 1

type executorTaskRunLockPathSchema map[schema.Version]string

var executorTaskRunLockPath = executorTaskRunLockPathSchema{
	1: "/runlock/{{ .TaskRunID }}",
}

func ExecutorTaskRunLockPath(id s.TaskRunID) string {
	path := executorTaskRunLockPath[latestExecutorTaskRunLockPath]
	templ := schema.Templater(path, map[string]interface{}{
		"TaskRunID": id.Value(),
	})
	return templ
}

// For future upgrades
type executorTaskRunLockPathUpgrader struct {
	executorTaskRunLockPathSchema
}

func (p *executorTaskRunLockPathUpgrader) Upgrade(start, end schema.Version) error {
	return nil
}

func (p *executorTaskRunLockPathUpgrader) Downgrade(start, end schema.Version) error {
	return nil
}

const latestExecutorTaskLockPath schema.Version = 1

type executorTaskLockPathSchema map[schema.Version]string

var executorTaskLockPath = executorTaskLockPathSchema{
	1: "/tasklock/{{ .TaskID }}",
}

func ExecutorTaskLockPath(id s.TaskID) string {
	path := executorTaskLockPath[latestExecutorTaskLockPath]
	templ := schema.Templater(path, map[string]interface{}{
		"TaskID": id.Value(),
	})
	return templ
}

// For future upgrades
type executorTaskLockPathUpgrader struct {
	executorTaskLockPathSchema
}

func (p *executorTaskLockPathUpgrader) Upgrade(start, end schema.Version) error {
	return nil
}

func (p *executorTaskLockPathUpgrader) Downgrade(start, end schema.Version) error {
	return nil
}
