// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package types

// SparkDeployMode tells Spark where the main application should run.
type SparkDeployMode string

const (
	// SparkClusterDeployMode tells spark to run the job with the main thread being on a random
	// node on the cluster.
	SparkClusterDeployMode SparkDeployMode = "cluster"
	// SparkClientDeployMode tells spark to run the job with the main thread being run externally
	// , in this case, on FF itself.
	SparkClientDeployMode SparkDeployMode = "client"
)

// SparkArg returns the string that should be passed in as the
// value of the --deploy-mode flag in spark_submit.
func (mode SparkDeployMode) SparkArg() string {
	return string(mode)
}

type SparkFileStoreType string

func (t SparkFileStoreType) String() string {
	return string(t)
}

const (
	SFS_S3         SparkFileStoreType = "s3"
	SFS_AZURE_BLOB SparkFileStoreType = "azure_blob_store"
	SFS_GCS        SparkFileStoreType = "google_cloud_storage"
	SFS_HDFS       SparkFileStoreType = "hdfs"
	SFS_LOCAL      SparkFileStoreType = "local"
	SFS_BIGLAKE    SparkFileStoreType = "biglake"
)
