package utils

func ArrayMinus(first []string, second []string) []string {
	lookup := make(map[string]struct{}, len(second))
	// Add elements from arrayB to the lookup map
	for _, element := range second {
		lookup[element] = struct{}{}
	}
	// Iterate over arrayA and check if the element is present in the lookup map
	var result []string
	for _, element := range first {
		_, exists := lookup[element]
		if !exists {
			result = append(result, element)
		}
	}
	return result
}
