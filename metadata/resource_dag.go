// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package metadata

import (
	"context"
	"fmt"

	"github.com/featureform/fferr"
	d "github.com/featureform/lib/dag"
	"github.com/featureform/scheduling"
)

func NewResourceDAG(ctx context.Context, lookup ResourceLookup, resource Resource) (ResourceDAG, error) {
	// Creates a new ResourceDAG
	genericDAG, err := d.NewGenericDAG()
	if err != nil {
		return ResourceDAG{}, err
	}

	dag := ResourceDAG{
		dag: genericDAG,
	}

	// Add the resource to the DAG
	dag.AddResource(resource)

	// Build the DAG
	err = buildResourceDAG(ctx, lookup, resource, dag)
	if err != nil {
		return ResourceDAG{}, err
	}

	return dag, nil
}

func buildResourceDAG(ctx context.Context, lookup ResourceLookup, resource Resource, dag ResourceDAG) error {
	// Builds a ResourceDAG from a resource and its dependencies
	deps, err := resource.Dependencies(ctx, lookup)
	if err != nil {
		return err
	}

	depsList, err := deps.List(ctx)
	if err != nil {
		return err
	}

	for _, dep := range depsList {
		dag.AddResource(dep)
		err := dag.AddDependency(resource, dep)
		if err != nil {
			return err
		}
		err = buildResourceDAG(ctx, lookup, dep, dag)
		if err != nil {
			return err
		}
	}

	return nil
}

type ResourceDAG struct {
	dag d.GenericDAG
}

func (r *ResourceDAG) Vertices() int {
	// Returns the number of vertices in the DAG
	return r.dag.Vertices()
}

func (r *ResourceDAG) AddResource(resource Resource) {
	// Adds a resource to the DAG
	r.dag.AddNode(resource)
}

func (r *ResourceDAG) AddDependency(parent Resource, child Resource) error {
	return r.dag.AddEdge(parent, child)
}

func (r *ResourceDAG) getResourceTaskImpl(resource Resource) (resourceTaskImplementation, error) {
	// Gets the resource task implementation
	res, ok := resource.(resourceTaskImplementation)
	if !ok {
		return nil, fferr.NewInternalErrorf("resource is not a task implementation: %v", resource)
	}

	return res, nil
}

func (r *ResourceDAG) ToTaskDAG() (scheduling.TaskDAG, error) {
	// Converts a ResourceDAG to a TaskDAG
	taskDAG, err := scheduling.NewTaskDAG()
	if err != nil {
		errMsg := fmt.Errorf("failed to convert to TaskDAG: %v", err)
		return scheduling.TaskDAG{}, fferr.NewInternalError(errMsg)
	}

	for parent, children := range r.dag.GetEdgesAndNode() {
		for _, child := range children {
			// TODO: Need to get the TaskID from the Resources
			p, err := r.getResourceTaskImpl(parent.(Resource))
			if err != nil {
				return scheduling.TaskDAG{}, err
			}
			c, err := r.getResourceTaskImpl(child.(Resource))
			if err != nil {
				return scheduling.TaskDAG{}, err
			}

			parentTaskIDs, err := p.TaskIDs()
			if err != nil {
				return scheduling.TaskDAG{}, err
			}
			childTaskIDs, err := c.TaskIDs()
			if err != nil {
				return scheduling.TaskDAG{}, err
			}

			err = r.addTaskDependency(&taskDAG, parentTaskIDs, childTaskIDs)
			if err != nil {
				errMsg := fmt.Errorf("failed to add task dependencies: %v", err)
				return scheduling.TaskDAG{}, fferr.NewInternalError(errMsg)
			}
		}
	}

	return taskDAG, nil
}

func (r *ResourceDAG) addTaskDependency(taskDAG *scheduling.TaskDAG, parent []scheduling.TaskID, child []scheduling.TaskID) error {
	// Adds a dependency between tasks in a TaskDAG
	for _, p := range parent {
		for _, c := range child {
			err := taskDAG.AddDependency(p, c)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *ResourceDAG) Dependencies(resource Resource) []Resource {
	// Returns the direct dependencies of a resource
	var deps []Resource
	for _, dep := range r.dag.GetNodeEdges(resource) {
		deps = append(deps, dep.(Resource))
	}
	return deps
}
