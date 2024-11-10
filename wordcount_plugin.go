// wordcount_plugin.go
package main

import (
	"fmt"
	"strings"
	"strconv"
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

// Updated Reduce function for word count
// Takes a key (word) and a slice of values (each as []byte), parses each to an integer, and emits the total count for the word.
func Reduce(key interface{}, values []interface{}, emit func(interface{}, interface{})) error {
	word, ok := key.(string)
	if !ok {
		return fmt.Errorf("expected string key, got %T", key)
	}

	// Sum up the counts for this word, treating each value as []byte
	totalCount := 0
	for _, value := range values {
		// Assert the type to []byte and parse it as an integer
		byteValue, ok := value.([]byte)
		if !ok {
			return fmt.Errorf("expected []byte value, got %T", value)
		}

		// Convert the []byte to an integer
		count, err := strconv.Atoi(string(byteValue))
		if err != nil {
			return fmt.Errorf("failed to parse count for key %s: %v", word, err)
		}

		totalCount += count
	}

	// Emit the final count for the word
	emit(word, totalCount)
	return nil
}

