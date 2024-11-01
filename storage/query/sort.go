// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package query

type SortDirection string

const (
	DefaultDirection SortDirection = ""
	Asc              SortDirection = "ASC"
	Desc             SortDirection = "DESC"
)

type Sort interface {
	Query
	Direction() SortDirection
}

type KeySort struct {
	// Direction to order, defaults ASC
	Dir SortDirection
}

func (opt KeySort) Direction() SortDirection {
	if opt.Dir == DefaultDirection {
		return Asc
	}
	return opt.Dir
}

func (opt KeySort) Category() Category {
	return SortQuery
}

type ValueSort struct {
	// Direction to order, defaults ASC
	Dir SortDirection
	// Column is optional, it specifies what part of the value to use.
	// If not included, we default to the whole value to sort by.
	Column Column
}

func (opt ValueSort) Category() Category {
	return SortQuery
}

func (opt ValueSort) Direction() SortDirection {
	if opt.Dir == DefaultDirection {
		return Asc
	}
	return opt.Dir
}
