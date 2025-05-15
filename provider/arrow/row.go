// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package arrow

import (
	"time"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"

	arrowlib "github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

func ConvertRecordToRows(rec arrowlib.Record, schema types.Schema) (types.Rows, error) {
	n := int(rec.NumRows())
	m := int(rec.NumCols())
	if len(schema.Fields) != m {
		return nil, fferr.NewInternalErrorf(
			"Record had %d columns, but schema %v doesn't", m, schema.Fields,
		)
	}
	rows := make(types.Rows, n)
	for i := range rows {
		rows[i] = make(types.Row, m)
	}
	arrSchema := rec.Schema()
	recSchema, err := ConvertSchema(arrSchema)
	if err != nil {
		return nil, fferr.NewInternalErrorf(
			"Record schema convert failed %w does not match expected schema %#v", err, schema,
		)
	}
	if !recSchema.Equals(schema) {
		return nil, fferr.NewInternalErrorf(
			"Record schema %#v does not match expected schema %#v", recSchema, schema,
		)
	}
	// For each column, convert the entire column to []any.
	for j := 0; j < m; j++ {
		colVals := convertArrayToValues(rec.Column(j), arrSchema.Field(j).Type)
		// Fill the j-th value of each row with the corresponding column value.
		for i := 0; i < n; i++ {
			rows[i][j] = types.Value{
				NativeType: schema.Fields[j].NativeType,
				Type:       schema.Fields[j].Type,
				Value:      colVals[i],
			}
		}
	}
	return rows, nil
}

func convertArrayToValues(arr arrowlib.Array, dt arrowlib.DataType) []any {
	out := make([]any, arr.Len())

	switch col := arr.(type) {

	// ------------------------------------------------------------------------
	// STRING / LARGE STRING
	// ------------------------------------------------------------------------
	// .Value(i) returns a Go 'string'.
	case *array.String:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.String => col.Value(i) is a string
			out[i] = col.Value(i)
		}

	case *array.LargeString:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.LargeString => col.Value(i) is a string
			out[i] = col.Value(i)
		}

	// ------------------------------------------------------------------------
	// BINARY / FIXED-SIZE BINARY
	// ------------------------------------------------------------------------
	// .Value(i) returns a Go '[]byte'.
	case *array.Binary:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.Binary => col.Value(i) is []byte
			out[i] = string(col.Value(i))
		}

	case *array.FixedSizeBinary:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.FixedSizeBinary => col.Value(i) is []byte of fixed length
			out[i] = string(col.Value(i))
		}

	// ------------------------------------------------------------------------
	// BOOLEAN
	// ------------------------------------------------------------------------
	// .Value(i) returns a Go 'bool'.
	case *array.Boolean:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.Boolean => col.Value(i) is bool
			out[i] = col.Value(i)
		}

	// ------------------------------------------------------------------------
	// SIGNED INTEGERS (Int8, Int16, Int32, Int64)
	// ------------------------------------------------------------------------
	// .Value(i) returns a Go int8 / int16 / int32 / int64.
	case *array.Int8:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.Int8 => col.Value(i) is int8
			out[i] = col.Value(i)
		}
	case *array.Int16:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.Int16 => col.Value(i) is int16
			out[i] = col.Value(i)
		}
	case *array.Int32:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.Int32 => col.Value(i) is int32
			out[i] = col.Value(i)
		}
	case *array.Int64:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.Int64 => col.Value(i) is int64
			out[i] = col.Value(i)
		}

	// ------------------------------------------------------------------------
	// UNSIGNED INTEGERS (Uint8, Uint16, Uint32, Uint64)
	// ------------------------------------------------------------------------
	// .Value(i) returns a Go uint8 / uint16 / uint32 / uint64.
	case *array.Uint8:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.Uint8 => col.Value(i) is uint8
			out[i] = col.Value(i)
		}
	case *array.Uint16:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.Uint16 => col.Value(i) is uint16
			out[i] = col.Value(i)
		}
	case *array.Uint32:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.Uint32 => col.Value(i) is uint32
			out[i] = col.Value(i)
		}
	case *array.Uint64:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.Uint64 => col.Value(i) is uint64
			out[i] = col.Value(i)
		}

	// ------------------------------------------------------------------------
	// FLOATS (Float32, Float64)
	// ------------------------------------------------------------------------
	// .Value(i) returns a Go float32 / float64.
	case *array.Float32:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.Float32 => col.Value(i) is float32
			out[i] = col.Value(i)
		}
	case *array.Float64:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			// array.Float64 => col.Value(i) is float64
			out[i] = col.Value(i)
		}

	// ------------------------------------------------------------------------
	// TIMESTAMP => arrowlib.Timestamp => underlying = int64
	// We must check the time unit (second, ms, us, ns) to produce a Go time.Time
	// .Value(i) returns arrowlib.Timestamp (alias for int64).
	case *array.Timestamp:
		tsType, ok := dt.(*arrowlib.TimestampType)
		for i := 0; i < col.Len(); i++ {
			if !ok || col.IsNull(i) {
				out[i] = nil
				continue
			}
			raw := col.Value(i) // arrowlib.Timestamp => int64
			switch tsType.Unit {
			case arrowlib.Second:
				out[i] = time.Unix(int64(raw), 0).UTC()
			case arrowlib.Millisecond:
				ms := int64(raw)
				out[i] = time.Unix(0, ms*1_000_000).UTC()
			case arrowlib.Microsecond:
				micros := int64(raw)
				out[i] = time.Unix(0, micros*1_000).UTC()
			case arrowlib.Nanosecond:
				nanos := int64(raw)
				out[i] = time.Unix(0, nanos).UTC()
			}
		}

	// ------------------------------------------------------------------------
	// DATE32 => days since epoch => returns a Go time.Time
	// .Value(i) => int32
	case *array.Date32:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			days := col.Value(i) // int32
			secs := int64(days) * 86400
			out[i] = time.Unix(secs, 0).UTC()
		}

	// ------------------------------------------------------------------------
	// DATE64 => milliseconds since epoch => returns a Go time.Time
	// .Value(i) => int64
	case *array.Date64:
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				out[i] = nil
				continue
			}
			ms := int64(col.Value(i)) // int64
			nanos := ms * 1_000_000
			out[i] = time.Unix(0, nanos).UTC()
		}

	// ------------------------------------------------------------------------
	// FIXED-SIZE LIST => e.g. each row is a sub-slice
	// We'll assume the child is float64 here. If child can be other types,
	// you'd recursively call Convert() on the child array.
	// .ListValues() => arrowlib.Array with all child data
	// TODO
	// TODO case *array.FixedSizeList:
	// TODO     listType, ok := dt.(*arrowlib.FixedSizeListType)
	// TODO     listSize := int(col.Len())
	// TODO     childArr := col.ListValues() // arrowlib.Array
	// TODO     // Suppose we expect float64 child
	// TODO     childF64 := childArr.(*array.Float64)
	// TODO     offset := 0
	// TODO     for i := 0; i < col.Len(); i++ {
	// TODO         if col.IsNull(i) {
	// TODO             out[i] = nil
	// TODO             offset += listSize
	// TODO             continue
	// TODO         }
	// TODO         // build a slice of float64
	// TODO         rowVals := make([]float64, listSize)
	// TODO         for j := 0; j < listSize; j++ {
	// TODO             if childF64.IsNull(offset + j) {
	// TODO                 // if child is null, we store 0.0 or nil
	// TODO                 // up to you. Let's store nil in a separate slice of any.
	// TODO                 // For simplicity, store 0.0 here.
	// TODO                 rowVals[j] = 0.0
	// TODO             } else {
	// TODO                 rowVals[j] = childF64.Value(offset + j)
	// TODO             }
	// TODO         }
	// TODO         out[i] = rowVals
	// TODO         offset += listSize
	// TODO     }

	// ------------------------------------------------------------------------
	// DEFAULT / UNHANDLED
	// ------------------------------------------------------------------------
	default:
		// For unrecognized array types (struct, map, decimal, etc.), store nil
		for i := 0; i < arr.Len(); i++ {
			out[i] = nil
		}
	}

	return out
}
