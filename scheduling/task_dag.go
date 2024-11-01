// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package scheduling

import (
	"fmt"

	"github.com/featureform/fferr"
	d "github.com/featureform/lib/dag"
)

func NewTaskDAG() (TaskDAG, error) {
	dag, err := d.NewGenericDAG()
	if err != nil {
		return TaskDAG{}, err
	}

	// Creates a new TaskDAG
	return TaskDAG{
		dag:    dag,
		runMap: make(map[TaskID]TaskRunID),
	}, nil
}

type TaskDAG struct {
	dag    d.GenericDAG
	runMap map[TaskID]TaskRunID
}

func (t *TaskDAG) AddTask(task TaskID) {
	t.dag.AddNode(task)
}

func (t *TaskDAG) AddDependency(parent TaskID, child TaskID) error {
	return t.dag.AddEdge(parent, child)
}

func (t *TaskDAG) SetTaskRunID(task TaskID, run TaskRunID) error {
	if exists := t.dag.NodeExists(task); !exists {
		return fferr.NewInternalError(fmt.Errorf("task %v not in DAG", task))
	}

	if _, exists := t.runMap[task]; exists {
		return fferr.NewInternalError(fmt.Errorf("task %v already has a run", task))
	}
	t.runMap[task] = run

	return nil
}

func (t *TaskDAG) GetTaskRunID(task TaskID) (TaskRunID, error) {
	run, exists := t.runMap[task]
	if !exists {
		return nil, fferr.NewInternalError(fmt.Errorf("task %v not in runMap", task))
	}
	return run, nil
}

func (t *TaskDAG) SortedTasks() []TaskID {
	// Returns a topological sort of the tasks in the DAG
	var tasks []TaskID
	for _, task := range t.dag.SortedNodes() {
		tasks = append(tasks, task.(TaskID))
	}
	return tasks
}

func (t *TaskDAG) Vertices() int {
	// Returns the number of vertices in the DAG
	return t.dag.Vertices()
}

func (r *TaskDAG) Dependencies(taskID TaskID) []TaskID {
	// Returns the direct dependencies of a resource
	var deps []TaskID
	for _, dep := range r.dag.GetNodeEdges(taskID) {
		deps = append(deps, dep.(TaskID))
	}
	return deps
}
