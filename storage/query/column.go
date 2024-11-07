// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package query

type ColumnType string

const (
	JSON ColumnType = "json"
	SQL  ColumnType = "sql"
)

type Column interface {
	ColumnType() ColumnType
}

type ValueType string

const (
	String    ValueType = "string"
	Int       ValueType = "int"
	Timestamp ValueType = "timestamp"
	Object    ValueType = "object"
)

type JSONPathStep struct {
	Key string
	// IsJsonString indicates that this step's value is a json encoded string.
	// We have cases where we have a JSON object, where one of its fields is a string type that's actually a JSON encoded string.
	// It may look like {"message": "\{\"inner_json\":\"example\"\}"}
	// Here the "message" value is a string, that contains json data, so IsJsonString should be true.
	IsJsonString bool
}

type JSONColumn struct {
	// Path to go through JSON structure.
	// Ex. ["a", "b"] would compile to
	// value.a.b
	// This does not have support arrays.
	Path []JSONPathStep
	Type ValueType
}

func (clm JSONColumn) ColumnType() ColumnType {
	return JSON
}

type SQLColumn struct {
	// this struct represents a table column
	Column string //base
	Alias  string //optional
}

func (clm SQLColumn) ColumnType() ColumnType {
	return SQL
}
