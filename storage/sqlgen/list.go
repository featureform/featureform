// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package sqlgen

import (
	"fmt"
	"strings"

	"github.com/featureform/fferr"
	"github.com/featureform/storage/query"
)

type List struct {
	TableName string
	Columns   []query.Column
	Filters   []query.Query
	Sort      query.Sort
	Limit     query.Limit
	GroupBy   query.GroupBy
}

func (list *List) Compile() (string, []any, error) {
	return list.compile(false)
}

func (list *List) CompileCount() (string, []any, error) {
	return list.compile(true)
}

func (list *List) compile(countOnly bool) (string, []any, error) {
	selectClms, err := list.compileColumns(countOnly)
	if err != nil {
		return "", nil, err
	}

	base := fmt.Sprintf("SELECT %s FROM %s", selectClms, list.TableName)
	queryParts := []string{
		base,
	}
	where, args, err := compileFilters(list.Filters)
	if err != nil {
		return "", nil, err
	}
	// If there's no filter to apply, don't add to parts
	if where != "" {
		queryParts = append(queryParts, where)
	}
	groupBy, err := compileGroupBy(list.GroupBy)
	if err != nil {
		return "", nil, err
	}
	if groupBy != "" {
		queryParts = append(queryParts, groupBy)
	}

	sort, err := compileSort(list.Sort)
	if err != nil {
		return "", nil, err
	}
	if sort != "" {
		queryParts = append(queryParts, sort)
	}
	limit, err := compileLimit(list.Limit)
	if err != nil {
		return "", nil, err
	}
	if limit != "" {
		queryParts = append(queryParts, limit)
	}

	qry := strings.Join(queryParts, " ")
	return qry, args, nil
}

func (list *List) compileColumns(countOnly bool) (string, error) {
	if countOnly {
		return "COUNT(*)", nil
	}

	// default to key,value
	if len(list.Columns) == 0 {
		return "key, value", nil
	}

	var clmStrings []string
	for _, clm := range list.Columns {
		switch clm.ColumnType() {
		case query.SQL:
			sqlCol, ok := clm.(query.SQLColumn)
			if !ok {
				return "", fmt.Errorf("failed to cast column to SQLColumn")
			}

			var column string
			if sqlCol.Column == "" {
				return "", fmt.Errorf("column cannot be empty string: %v", clm.ColumnType())
			}

			column = sqlCol.Column
			if sqlCol.Alias != "" {
				column = fmt.Sprintf("%s AS %s", column, sqlCol.Alias)
			}

			clmStrings = append(clmStrings, column)

		default:
			return "", fmt.Errorf("unsupported column type: %v", clm.ColumnType())
		}
	}

	return strings.Join(clmStrings, ", "), nil
}

func NewListQuery(tableName string, opts []query.Query, columns ...query.Column) (*List, error) {
	if tableName == "" {
		return nil, fferr.NewInternalErrorf("Table name not set in list query")
	}

	list := &List{
		TableName: tableName,
		Columns:   columns,
	}

	var err error
	if list.Sort, err = extractSortQuery(opts); err != nil {
		return nil, err
	}
	if list.Limit, err = extractLimitQuery(opts); err != nil {
		return nil, err
	}
	if list.GroupBy, err = extractGroupByQuery(opts); err != nil {
		return nil, err
	}

	list.Filters = extractFilterQueries(opts)

	return list, nil
}

func extractSortQuery(opts []query.Query) (query.Sort, error) {
	var hasSort bool
	var srt query.Sort
	for _, opt := range opts {
		if opt.Category() == query.SortQuery {
			if hasSort {
				return nil, fferr.NewInternalErrorf("Multiple sort queries set: %v", opts)
			}
			if srt, ok := opt.(query.Sort); ok {
				hasSort = true
				return srt, nil
			} else {
				return nil, fferr.NewInternalErrorf("Unable to cast sort query: %+v", opt)
			}
		}
	}
	return srt, nil
}

func extractLimitQuery(opts []query.Query) (query.Limit, error) {
	var hasLimit bool
	for _, opt := range opts {
		if opt.Category() == query.LimitQuery {
			if hasLimit {
				return query.Limit{}, fferr.NewInternalErrorf("Multiple limit queries set: %v", opts)
			}
			if limit, ok := opt.(query.Limit); ok {
				hasLimit = true
				return limit, nil
			} else {
				return query.Limit{}, fferr.NewInternalErrorf("Unable to cast limit query: %+v", opt)
			}
		}
	}
	return query.Limit{}, nil
}

func extractGroupByQuery(opts []query.Query) (query.GroupBy, error) {
	var hasGroupBy bool
	for _, opt := range opts {
		if opt.Category() == query.GroupQuery {
			if hasGroupBy {
				return query.GroupBy{}, fferr.NewInternalErrorf("Multiple group by queries set: %v", opts)
			}
			if groupBy, ok := opt.(query.GroupBy); ok {
				hasGroupBy = true
				return groupBy, nil
			} else {
				return query.GroupBy{}, fferr.NewInternalErrorf("Unable to cast group by query: %+v", opt)
			}
		}
	}
	return query.GroupBy{}, nil
}

func extractFilterQueries(opts []query.Query) []query.Query {
	var filters []query.Query
	for _, opt := range opts {
		if opt.Category() == query.FilterQuery {
			filters = append(filters, opt)
		}
	}
	return filters
}
