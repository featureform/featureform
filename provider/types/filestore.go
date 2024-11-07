// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package types

type FileStoreType string

const (
	UNKNOWN_FILE_STORE FileStoreType = ""
	S3Type                           = "s3"
)

type TableFormatType string

const (
	UNKNOWN_TABLE_FORMAT TableFormatType = ""
	DeltaType                            = "delta"
	IcebergType                          = "iceberg"
)
