// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package spark

import (
	"reflect"
	"testing"

	"github.com/featureform/filestore"
)

func TestSparkConfig(t *testing.T) {
	path, err := filestore.NewEmptyFilepath(filestore.S3)
	if err != nil {
		t.Fatalf("Failed to create empty file path: %s", err)
	}
	type testCase struct {
		Configs  Configs
		Expected []string
	}

	testCases := map[string]testCase{
		"SimpleIceberg": testCase{
			Configs: Configs{IcebergFlags{}},
			Expected: []string{
				"spark-submit",
				"--packages",
				"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
				"/",
				"--spark_config",
				"\"spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\"",
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			flags := test.Configs.CompileCommand(path)
			if !reflect.DeepEqual(flags, test.Expected) {
				t.Fatalf("Flags not equal.\n%v\n%v\n", flags, test.Expected)
			}
		})
	}
}
