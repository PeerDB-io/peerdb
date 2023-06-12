package utils

func ArrayMinus(first []string, second []string) []string {
	lookup := make(map[string]bool)
	// Add elements from arrayB to the lookup map
	for _, element := range second {
		lookup[element] = true
	}
	// Iterate over arrayA and check if the element is present in the lookup map
	var result []string
	for _, element := range first {
		if !lookup[element] {
			result = append(result, element)
		}
	}
	return result
}
