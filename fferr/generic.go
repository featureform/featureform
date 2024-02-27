package fferr

import (
	"fmt"
	"strings"

	"github.com/rotisserie/eris"
)

const ENABLE_STACK_TRACE = true

func NewGenericError(err error) GenericError {
	msg := err.Error()
	return GenericError{
		msg:     msg,
		err:     eris.New(msg),
		details: map[string]string{},
	}
}

type GenericError struct {
	msg        string
	err        error
	details    map[string]string
	detailKeys []string
}

func (e *GenericError) Error() string {
	return e.msg
}

func (e *GenericError) Stack() JSONStackTrace {
	return eris.ToJSON(e.err, ENABLE_STACK_TRACE)
}

func (e *GenericError) Details() map[string]string {
	return e.details
}

func (e *GenericError) AddDetail(key, value string) {
	key = strings.ReplaceAll(key, " ", "_")
	key = strings.ToLower(key)
	e.details[key] = value
	e.detailKeys = append(e.detailKeys, key)
}

func (e *GenericError) SetMessage(msg string) {
	e.msg = fmt.Sprintf("%s: %s", msg, e.msg)
}
