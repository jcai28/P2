package main

import (
	"fmt"
	"strconv"
	"strings"
)

// Mapper function: Extracts domains from each log line and emits (domain, 1)
func Map(key interface{}, value interface{}, emit func(interface{}, interface{})) error {
	// Ensure the value is in []byte format
	data, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte value, got %T", value)
	}

	// Convert the byte slice to a string
	line := string(data)

	// Split the line into fields
	fields := strings.Fields(line)
	if len(fields) < 4 {
		// Skip lines with insufficient fields
		return nil
	}

	// Extract the domain from the URL (4th field)
	rawURL := fields[3]
	parts := strings.Split(rawURL, "/")
	if len(parts) < 3 {
		// Skip malformed URLs
		return nil
	}

	domain := strings.ToLower(parts[2]) // Normalize domain to lowercase

	// Emit the domain with a count of 1
	emit(domain, 1)
	return nil
}


// Reducer function: Aggregates counts for each domain
func Reduce(key interface{}, values []interface{}, emit func(interface{}, interface{})) error {
	// Ensure the key is a string (domain)
	domain, ok := key.(string)
	if !ok {
		return fmt.Errorf("expected string key, got %T", key)
	}

	// Sum the counts for this domain
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
			return fmt.Errorf("failed to parse count for domain %s: %v", domain, err)
		}

		totalCount += count
	}

	// Emit the final count for the domain
	emit(domain, totalCount)
	return nil
}
