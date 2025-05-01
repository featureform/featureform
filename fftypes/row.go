package types

import servpb "github.com/featureform/proto"

type Row []Value

func (row Row) ToProto() (*servpb.SourceDataRow, error) {
	values := make([]*servpb.Value, len(row))

	for i, val := range row {
		protoVal, err := val.ToProto()
		if err != nil {
			return nil, err
		}
		values[i] = protoVal
	}

	return &servpb.SourceDataRow{Rows: values}, nil
}
