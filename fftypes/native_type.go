package types

type NewNativeType interface {
	String() string
	IsNativeType() bool
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

func (n NativeTypeLiteral) String() string {
	return string(n)
}

func (n NativeTypeLiteral) IsNativeType() bool {
	return true
}
