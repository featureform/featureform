// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package query

type KeyPrefix struct {
	// Negates the filter, so filters everything
	// that does have the prefix.
	Not    bool
	Prefix string
}

func (qry KeyPrefix) Category() Category {
	return FilterQuery
}

type ValueEquals struct {
	Not    bool
	Column Column
	Value  any
}

func (qry ValueEquals) Category() Category {
	return FilterQuery
}

type ValueIn struct {
	Column Column
	Values []any
}

func (qry ValueIn) Category() Category {
	return FilterQuery
}

type ArrayContains struct {
	Column Column
	Values []any
}

func (qry ArrayContains) Category() Category {
	return FilterQuery
}

type ObjectArrayContains struct {
	Column      Column
	Values      []any
	SearchField string
}

func (qry ObjectArrayContains) Category() Category {
	return FilterQuery
}

type ValueLike struct {
	Column Column
	Value  any
}

func (qry ValueLike) Category() Category {
	return FilterQuery
}

type ConditionalOR struct {
	Filters []Query
}

func (qry ConditionalOR) Category() Category {
	return FilterQuery
}
