// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package core

import (
	"context"
	"testing"

	"github.com/featureform/logging"
)

type Context struct {
	context.Context
	logging.Logger
}

func NewContext(ctx context.Context, logger logging.Logger) *Context {
	return &Context{
		Context: ctx,
		Logger:  logger,
	}
}

func NewTestContext(t *testing.T) *Context {
	ctx, logger := logging.NewTestContextAndLogger(t)
	return &Context{
		Context: ctx,
		Logger:  logger,
	}
}
