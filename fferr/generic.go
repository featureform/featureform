// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

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
	// keys and values are expected to be in pairs, with the key first and the value second and key required to be a string

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
		// Convert the value to a string, this assumes that the value has a meaningful string representation
		value := fmt.Sprintf("%v", keysAndValues[i+1])
		e.AddDetail(key, value)
	}
}

func (e *GenericError) SetMessage(msg string) {
	e.msg = fmt.Sprintf("%s: %s", msg, e.msg)
}
