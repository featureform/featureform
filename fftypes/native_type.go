package types

type NewNativeType interface {
	String() string
	IsNativeType() bool
}

type NativeTypeDetails interface {
	ColumnName() string
}

type NativeTypeLiteral string

func (n NativeTypeLiteral) String() string {
	return string(n)
}

func (n NativeTypeLiteral) IsNativeType() bool {
	return true
}
