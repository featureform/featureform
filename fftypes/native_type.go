package types

type NativeType interface {
	TypeName() string
}

type NativeTypeDetails interface {
	ColumnName() string
}

type SimpleNativeTypeDetails struct {
	colName string
}

func NewSimpleNativeTypeDetails(colName string) SimpleNativeTypeDetails {
	return SimpleNativeTypeDetails{
		colName: colName,
	}
}

func (s SimpleNativeTypeDetails) ColumnName() string {
	return s.colName

}

type NativeTypeLiteral string

func (n NativeTypeLiteral) TypeName() string {
	return string(n)
}
