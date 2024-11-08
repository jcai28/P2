// wordcount_plugin.go
package main

import (
	"fmt"
	"strings"
)

// Map function for word count
// Takes a key (e.g., chunk ID) and a value (text content in bytes), and emits word counts.
func Map(key interface{}, value interface{}, emit func(interface{}, interface{})) error {
	// Ensure the value is in []byte format and convert it to string
	data, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte value, got %T", value)
	}
	text := string(data) // Convert byte data to string

	// Split the text into words
	words := strings.Fields(text)
	for _, word := range words {
		word = strings.ToLower(word) // Normalize to lowercase
		emit(word, 1)                // Emit (word, 1) for each word found
	}

	return nil
}
