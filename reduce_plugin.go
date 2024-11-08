// reduce_plugin.go
package main

import (
	"fmt"
)

// Reduce function for word count
// Takes a key (word) and a slice of values (counts), and emits the total count for the word.
func Reduce(key interface{}, values []interface{}, emit func(interface{}, interface{})) error {
	word, ok := key.(string)
	if !ok {
		return fmt.Errorf("expected string key, got %T", key)
	}

	// Sum up the counts for this word
	totalCount := 0
	for _, value := range values {
		count, ok := value.(int)
		if !ok {
			return fmt.Errorf("expected int value, got %T", value)
		}
		totalCount += count
	}

	// Emit the final count for the word
	emit(word, totalCount)
	return nil
}
