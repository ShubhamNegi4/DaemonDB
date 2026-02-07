package executor

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func toInt(v any) (int32, error) {
	switch x := v.(type) {
	case int:
		return int32(x), nil
	case int32:
		return x, nil
	case int64:
		return int32(x), nil
	case float64:
		return int32(x), nil
	case float32:
		return int32(x), nil
	case string:
		i, err := strconv.Atoi(x)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %q to int", x)
		}
		return int32(i), nil
	case []byte:
		s := strings.TrimSpace(string(x))
		i, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %q to int", s)
		}
		return int32(i), nil
	default:
		return 0, fmt.Errorf("expected int, got %T", v)
	}
}

func toString(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return strings.TrimSpace(string(x)), nil
	case int, int32, int64:
		return fmt.Sprintf("%d", x), nil
	case float32, float64:
		return fmt.Sprintf("%g", x), nil
	default:
		return "", fmt.Errorf("expected string, got %T", v)
	}
}

func toFloat(v any) (float32, error) {
	switch x := v.(type) {
	case float64:
		return float32(x), nil
	case float32:
		return x, nil
	case int:
		return float32(x), nil
	case int32:
		return float32(x), nil
	case int64:
		return float32(x), nil
	case string:
		f, err := strconv.ParseFloat(x, 32)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %q to float", x)
		}
		return float32(f), nil
	case []byte:
		s := strings.TrimSpace(string(x))
		f, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %q to float", s)
		}
		return float32(f), nil
	default:
		return 0, fmt.Errorf("expected float, got %T", v)
	}
}

func isInteger(v reflect.Value) bool {
	kind := v.Kind()
	return kind >= reflect.Int && kind <= reflect.Int64
}

func isFloat(v reflect.Value) bool {
	kind := v.Kind()
	return kind == reflect.Float32 || kind == reflect.Float64
}

func compareValues(v1, v2 interface{}) int {
	if v1 == nil || v2 == nil {
		if v1 == v2 {
			return 0
		}
		if v1 == nil {
			return -1
		}
		return 1
	}

	val1 := reflect.ValueOf(v1)
	val2 := reflect.ValueOf(v2)

	switch {
	case isInteger(val1) && isInteger(val2):
		i1, i2 := val1.Int(), val2.Int()
		if i1 < i2 {
			return -1
		}
		if i1 > i2 {
			return 1
		}
		return 0
	case isFloat(val1) || isFloat(val2):
		var f1, f2 float64
		if isFloat(val1) {
			f1 = val1.Float()
		} else {
			f1 = float64(val1.Int())
		}
		if isFloat(val2) {
			f2 = val2.Float()
		} else {
			f2 = float64(val2.Int())
		}
		if f1 < f2 {
			return -1
		}
		if f1 > f2 {
			return 1
		}
		return 0
	default:
		s1, s2 := fmt.Sprintf("%v", v1), fmt.Sprintf("%v", v2)
		if s1 < s2 {
			return -1
		}
		if s1 > s2 {
			return 1
		}
		return 0
	}
}
