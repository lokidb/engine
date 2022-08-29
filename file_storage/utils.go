package filestore

func equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}

	return true
}

// Scan file and return index of {key: file-offset}
func createKeysIndex(filename string) (*map[string]int64, int, error) {
	file := openOrCreate(filename)
	defer file.Close()

	keysIndex := make(map[string]int64)
	deletedKeysCount := 0

	err := scanFile(file, false, func(key string, value []byte, deleted bool, filePosition int64) {
		if deleted {
			deletedKeysCount++
		} else {
			keysIndex[key] = filePosition
		}
	})

	return &keysIndex, deletedKeysCount, err
}
