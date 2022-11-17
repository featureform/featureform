package provider

type InvalidQueryError struct {
	error string
}

func (e InvalidQueryError) Error() string {
	return e.error
}

type SQLError struct {
	error error
}

func (e SQLError) Error() string {
	return e.error.Error()
}

type TransformationTypeError struct {
	error string
}

func (e TransformationTypeError) Error() string {
	return e.error
}

type PrimaryTableAlreadyExists struct {
	id ResourceID
}

func (err *PrimaryTableAlreadyExists) Error() string {
	return fmt.Sprintf("Primary Table %s Variant %s already exists.", err.id.Name, err.id.Variant)
}
