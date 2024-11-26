// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider_config

import (
	ss "github.com/featureform/helpers/stringset"
	si "github.com/featureform/helpers/struct_iterator"
	sm "github.com/featureform/helpers/struct_map"
	"github.com/featureform/secrets"
)

type FileStoreConfig interface {
	Serialize() ([]byte, error)
	Deserialize(config SerializedConfig) error
	IsFileStoreConfig() bool
}

type ExecutorType string

type SerializedConfig []byte

func differingFields(a, b interface{}) (ss.StringSet, error) {
	diff := ss.StringSet{}
	aIter, err := si.NewStructIterator(a)
	if err != nil {
		return nil, err
	}

	bMap, err := sm.NewStructMap(b)

	if err != nil {
		return nil, err
	}

	for aIter.Next() {
		key := aIter.Key()
		aVal := aIter.Value()
		if !bMap.Has(key, aVal) {
			diff[key] = true
		}
	}

	return diff, nil
}

type ResolvableConfig interface {
	ResolveValues(secretsManagers secrets.Manager) error
}
