// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package scheduling

import (
	"github.com/featureform/ffsync"
	"github.com/featureform/schema"
)

const latestUnfinishedRunPath schema.Version = 1

type unfinishedTaskRunPathSchema map[schema.Version]string

var unfinishedTaskRunPath = unfinishedTaskRunPathSchema{
	1: "/tasks/incomplete/runs/run_id={{ .TaskRunID }}",
}

func (p unfinishedTaskRunPathSchema) Prefix() string {
	path := unfinishedTaskRunPath[latestUnfinishedRunPath]
	templ := schema.Templater(path, map[string]interface{}{
		"TaskRunID": "",
	})
	return templ
}

func (p unfinishedTaskRunPathSchema) Parse(given string) (TaskRunID, error) {
	path := unfinishedTaskRunPath[latestUnfinishedRunPath]
	template, err := schema.ParseTemplate(path, given)
	if err != nil {
		return TaskRunID(ffsync.Uint64OrderedId(0)), nil
	}
	id, err := ParseTaskRunID(template["TaskRunID"])
	if err != nil {
		return nil, err
	}
	return id, nil
}

func UnfinishedTaskRunPath(id TaskRunID) string {
	path := unfinishedTaskRunPath[latestUnfinishedRunPath]
	templ := schema.Templater(path, map[string]interface{}{
		"TaskRunID": id.Value(),
	})
	return templ
}

// For future upgrades
type unfinishedTaskRunPathUpgrader struct {
	unfinishedTaskRunPathSchema
}

func (p *unfinishedTaskRunPathUpgrader) Upgrade(start, end schema.Version) error {
	return nil
}

func (p *unfinishedTaskRunPathUpgrader) Downgrade(start, end schema.Version) error {
	return nil
}
