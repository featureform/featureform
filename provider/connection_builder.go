// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"fmt"
	"net/url"

	"github.com/featureform/logging"
	"github.com/featureform/provider/provider_config"
)

var logger = logging.NewLogger("connection_builder")

func PostgresConnectionBuilder(sc provider_config.PostgresConfig) string {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", sc.Username, sc.Password, sc.Host, sc.Port, sc.Database, sc.SSLMode)

	// Append the search_path only if the schema is not empty
	if sc.Schema != "" {
		connStr += fmt.Sprintf("&search_path=%s", url.QueryEscape(fmt.Sprintf(`"%s"`, sc.Schema)))
	}

	return connStr
}

func PostgresConnectionBuilderFunc(sc provider_config.PostgresConfig) func(database string, schema string) (string, error) {
	return func(database, schema string) (string, error) {
		// We make a copy so that the original struct is not modified within this closure
		// If we don't than anyone calling the returned function will modify the enclosure struct
		scCopy := sc

		// If database and schema are passed in we return a connection string with the new values
		if database != "" {
			scCopy.Database = database
		}
		if schema == "" {
			scCopy.Schema = "public"
		} else {
			scCopy.Schema = schema
		}
		return PostgresConnectionBuilder(scCopy), nil
	}
}
