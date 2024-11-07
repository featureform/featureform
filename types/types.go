// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package types

import (
	"github.com/featureform/metadata"
)

type Runner interface {
	Run() (CompletionWatcher, error)
	Resource() metadata.ResourceID
	IsUpdateJob() bool
}

type IndexRunner interface {
	Runner
	SetIndex(index int) error
}

type CompletionWatcher interface {
	Complete() bool
	String() string
	Wait() error
	Err() error
}
