// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package types

// basic representation of our featureform types,
// using iota for now, doesn't need an actual ordering for the moment
// todo: will change name later
type FF_Type int

const (
	NilType FF_Type = iota
	Int
	Int8
	Int16
	Int32
	Int64
	UInt8
	UInt16
	UInt32
	UInt64
	Float32
	Float64
	String
	Bool
	Timestamp
	Datetime
)

// todo: core structs. update the Value to fit a better type representation other than any/interface?
type Value struct {
	// Provider   string      // where the value originated from, can be enumeration note: we may not need this extra context
	Name       string      // the name of the value
	NativeType string      // todo: this will be some constant. is the provider's type (DynamoDB INT64)
	Type       FF_Type     // this is our type
	Value      interface{} // "FFValue" may update later to a diff type with extensible receiver methods
}

// todo: a row represents a list of featureform values, can also have extension methods
type Row []Value
