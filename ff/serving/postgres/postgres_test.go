package postgres

import (
	"testing"
)

func TestMinimalConnectionParam(t *testing.T) {
	params := ConnectionParams{
		User:   "abc",
		DBName: "db",
		Host:   "localhost",
	}
	str := params.toURLString()
	expected := "postgres://abc:@localhost:5432/db?sslmode=disable"
	if str != expected {
		t.Errorf(
			"Connection Parameters mis-encoded.\nExpected: %s\n Got: %s\n",
			expected, str,
		)
	}
}
