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

func (e *GenericError) AddDetails(keysAndValues ...interface{}) {
	// Modeled after a simple version of the logging.infow method

	if len(keysAndValues)%2 != 0 {
		// We're just going to print an error if the number of arguments is odd and omit the last one.
		fmt.Println("AddDetails called with odd number of arguments, expected key-value pairs")
	}

	n := len(keysAndValues)
	if n%2 != 0 {
		n = n - 1 // Skip the last one if odd
	}

	for i := 0; i < n; i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			// Handle or log the error if the key is not a string.
			fmt.Printf("Key at index %d is not a string\n", i)
			continue
		}
		value, ok := keysAndValues[i+1].(string)
		if !ok {
			// Handle or log the error if the value is not a string.
			fmt.Printf("Value at index %d is not a string\n", i+1)
			continue
		}
		e.AddDetail(key, value)
	}
}

func (e *GenericError) SetMessage(msg string) {
	e.msg = fmt.Sprintf("%s: %s", msg, e.msg)
}
