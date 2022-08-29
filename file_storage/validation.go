package filestore

import (
	"fmt"
)

// Check if the key is valid, returns error for invalid key
func isValidKey(key string) error {
	if key == "" {
		return fmt.Errorf("can't set empty key")
	}

	if len(key) > maxKeyLenght {
		return fmt.Errorf("key lenght is grater then %d", maxKeyLenght)
	}

	return nil
}

// Check if the value is valid, returns error for invalid value
func isValidValue(value []byte) error {
	if len(value) == 0 {
		return fmt.Errorf("can't set empty value or nil value")
	} else if len(value) > maxValueLenght {
		return fmt.Errorf("value lenght is grater then %d", maxValueLenght)
	}

	return nil
}
