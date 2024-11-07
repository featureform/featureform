// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package retriever

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeserializeValueProvider(t *testing.T) {
	tests := []struct {
		name          string
		valueProvider ValueProvider
		wantErr       bool
	}{
		{
			name:          "EnvironmentValueProvider",
			valueProvider: &EnvironmentValueProvider{},
			wantErr:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serialized, err := tt.valueProvider.Serialize()
			assert.NoError(t, err)

			deserialized, err := DeserializeValueProvider(serialized)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.valueProvider, deserialized)
		})
	}
}
