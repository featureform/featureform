// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package spark

import (
	"encoding/json"

	"github.com/featureform/fferr"
	pt "github.com/featureform/provider/provider_type"
)

type SourceInfo struct {
	Location     string  `json:"location"`
	LocationType string  `json:"locationType"`
	Provider     pt.Type `json:"provider"`
	// TableFormat is used for file sources
	TableFormat string `json:"tableFormat"`
	// FileType and IsDir are used for file sources
	FileType string `json:"fileType"`
	IsDir    bool   `json:"isDir"`
	// Database and Schema are used for Snowflake sources
	Database string `json:"database"`
	Schema   string `json:"schema"`

	// AwsAssumeRoleArn is used for S3/Glue sources that
	// require a specific role to access
	AwsAssumeRoleArn    string `json:"awsAssumeRoleArn"`
	TimestampColumnName string `json:"timestampColumnName"`

	// Deprecated
	// TODO remove
	// Old version of our pyspark job actually passed in strings
	// as opposed to JSON for source infos. If legacy string is
	// set than serialization will just return this string.
	LegacyString string `json:"-"`
}

// Legacy parts of our script, specifically materialization, don't use the
// JSON version of pyspark source info.
func WrapLegacySourceInfos(paths []string) []SourceInfo {
	sources := make([]SourceInfo, len(paths))
	for i, path := range paths {
		sources[i] = SourceInfo{
			LegacyString: path,
		}
	}
	return sources
}

func (p *SourceInfo) Serialize() (string, error) {
	if p.LegacyString != "" {
		return p.LegacyString, nil
	}
	jsonBytes, err := json.Marshal(p)
	if err != nil {
		return "", fferr.NewInternalError(err)
	}
	return string(jsonBytes), nil
}