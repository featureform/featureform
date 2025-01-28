// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package scheduling

import (
	"github.com/featureform/logging"
	"testing"

	help "github.com/featureform/helpers"
)

func TestGetEmptyTasksWithETCD(t *testing.T) {
	if testing.Short() {
		t.Skip("Integration Test")
	}
	etcdConfig := help.ETCDConfig{
		Host: "localhost",
		Port: "2379",
	}

	manager, err := NewETCDTaskMetadataManager(etcdConfig)
	if err != nil {
		t.Fatalf(err.Error())
	}

	_, err = manager.GetAllTasks()
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestGetEmptyTasksWithPSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Integration Test")
	}

	psqlConfig := help.PSQLConfig{
		Host:     help.GetEnv("POSTGRES_HOST", "localhost"),
		Port:     help.GetEnv("POSTGRES_PORT", "5432"),
		User:     help.GetEnv("POSTGRES_USER", "postgres"),
		Password: help.GetEnv("POSTGRES_PASSWORD", "password"),
		DBName:   help.GetEnv("POSTGRES_DB", "postgres"),
		SSLMode:  help.GetEnv("POSTGRES_SSL_MODE", "disable"),
	}

	manager, err := NewPSQLTaskMetadataManager(psqlConfig, logging.NewTestLogger(t))
	if err != nil {
		t.Fatalf(err.Error())
	}

	_, err = manager.GetAllTasks()
	if err != nil {
		t.Fatalf(err.Error())
	}
}
