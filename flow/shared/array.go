package shared

// first - second
func ArrayMinus[T comparable](first, second []T) []T {
	lookup := make(map[T]struct{}, len(second))
	// Add elements from arrayB to the lookup map
	for _, element := range second {
		lookup[element] = struct{}{}
	}
	// Iterate over arrayA and check if the element is present in the lookup map
	var result []T
	for _, element := range first {
		_, exists := lookup[element]
		if !exists {
			result = append(result, element)
		}
	}
	return result
}

func ArrayCastElements[T any](arr []any) []T {
	res := make([]T, 0, len(arr))
	for _, val := range arr {
		v, _ := val.(T)
		res = append(res, v)
	}
	return res
}
