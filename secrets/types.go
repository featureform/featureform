package secrets

import (
	"fmt"
	"strconv"
)

type SupportedTypes interface {
	~string | ~int | ~bool
}

func convertStringToType[T SupportedTypes](valueStr string) (T, error) {
	var t T // this is the zero value of T
	switch any(t).(type) {
	case string:
		return any(valueStr).(T), nil
	case int:
		parsed, err := strconv.Atoi(valueStr)
		if err != nil {
			return t, fmt.Errorf("error parsing %q as int: %v", valueStr, err)
		}
		return any(parsed).(T), nil
	case bool:
		parsed, err := strconv.ParseBool(valueStr)
		if err != nil {
			return t, fmt.Errorf("error parsing %q as bool: %v", valueStr, err)
		}
		return any(parsed).(T), nil
	default:
		return t, fmt.Errorf("unsupported type %T", t)
	}
}
