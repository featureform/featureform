// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package clickhouse

import (
	"fmt"
	"strings"

	"github.com/featureform/fferr"
	pl "github.com/featureform/provider/location"
)

type Location struct {
	*pl.SQLLocation
}

func NewLocation(location pl.Location) (*Location, error) {
	switch loc := location.(type) {
	case *Location:
		return loc, nil
	case *pl.SQLLocation:
		if loc.GetSchema() != "" {
			return nil, fferr.NewInvalidArgumentErrorf("clickhouse location must not have schema, location: %v", location)
		}
		return &Location{SQLLocation: loc}, nil
	default:
		return nil, fmt.Errorf("invalid location type for ClickHouse Location, got %T", loc)
	}
}

func NewLocationFromParts(dbName, tableName string) *Location {
	return &Location{SQLLocation: pl.NewFullyQualifiedSQLLocation(dbName, "", tableName)}
}

func (l *Location) IsRelative() bool {
	return l.GetDatabase() == ""
}

func (l *Location) IsAbsolute() bool {
	return l.GetDatabase() != ""
}

func (l *Location) Location() string {
	parts := []string{}
	if l.GetDatabase() != "" {
		parts = append(parts, l.GetDatabase())
	}

	parts = append(parts, l.GetTable())

	for i := range parts {
		parts[i] = "`" + parts[i] + "`"
	}

	return strings.Join(parts, ".")
}
