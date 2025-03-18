// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package arrow

import (
	"github.com/featureform/core"
	"github.com/featureform/provider/dataset"
	"github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	"github.com/featureform/streamer"
	"github.com/featureform/types"
)

type StreamerDataset struct {
	// TODO move this out of struct
	Ctx    *core.Context
	Client *streamer.Client
	Loc    location.Location
	Config pc.SparkConfig
	Limit  int64
}

func (ds *StreamerDataset) Location() location.Location {
	return ds.Loc
}

func (ds *StreamerDataset) Iterator() (dataset.Iterator, error) {
	req, err := streamer.RequestFromSparkConfig(ds.Ctx, ds.Config, ds.Loc)
	if err != nil {
		ds.Ctx.Errorw("Failed to get request from spark config", "err", err)
		return nil, err
	}
	arrSchema, err := ds.Client.GetSchema(ds.Ctx, req)
	if err != nil {
		ds.Ctx.Errorw("Failed to get schema", "loc", ds.Loc, "err", err)
		return nil, err
	}
	schema, err := ConvertSchema(arrSchema)
	if err != nil {
		ds.Ctx.Errorw("Failed to convert schema", "loc", ds.Loc, "arrSchema", arrSchema, "err", err)
		return nil, err
	}
	reader, err := ds.Client.GetReader(ds.Ctx, req, streamer.DatasetOptions{Limit: ds.Limit})
	if err != nil {
		ds.Ctx.Errorw("Failed to get reader", "loc", ds.Loc, "err", err)
		return nil, err
	}
	return newIterator(reader, schema)
}

func (ds *StreamerDataset) Schema() (types.Schema, error) {
	req, err := streamer.RequestFromSparkConfig(ds.Ctx, ds.Config, ds.Loc)
	if err != nil {
		ds.Ctx.Errorw("Failed to get request from spark config", "err", err)
		return types.Schema{}, err
	}
	arrSchema, err := ds.Client.GetSchema(ds.Ctx, req)
	if err != nil {
		ds.Ctx.Errorw("Failed to get schema", "loc", ds.Loc, "err", err)
		return types.Schema{}, err
	}
	schema, err := ConvertSchema(arrSchema)
	if err != nil {
		ds.Ctx.Errorw("Failed to convert schema", "loc", ds.Loc, "arrSchema", arrSchema, "err", err)
		return types.Schema{}, err
	}
	return schema, nil
}
