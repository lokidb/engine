// Package filestore provide thraed-safe interface for storing key value pairs on a single file
// the package includes automatic deleted keys cleanup when the number of deleted
// keys become more then 30% of the total count.
package filestore

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"sync"

	"github.com/lokidb/engine/cursor"
)

const maxKeyLenght = 255
const maxValueLenght = 16777214
const itemHeaderLenght = 5
const filePermissions = 600
const cleanupOnDeletedRatio = 0.3
const minDeletedKeyForCleanup = 500
const cleanFileExtension = ".clean"

type FileKeyValueStore struct {
	filePath        string
	keysIndex       map[string]int64
	deletedKeyCount int
	lock            sync.Mutex
}

func New(filePath string) *FileKeyValueStore {
	fs := new(FileKeyValueStore)
	fs.filePath = filePath

	ctx := context.Background()
	keysIndex, deletedKeysCount, err := createKeysIndex(ctx, filePath)
	if err != nil {
		panic(err)
	}

	fs.deletedKeyCount = deletedKeysCount
	fs.keysIndex = *keysIndex

	return fs
}

func openOrCreate(filePath string) *os.File {
	file, err := os.OpenFile(filePath, os.O_RDWR, os.FileMode(filePermissions))
	if err != nil {
		file, err = os.Create(filePath)
	}

	if err != nil {
		panic(err)
	}

	return file
}

func (fst *FileKeyValueStore) openOrPanic() *os.File {
	file, err := os.OpenFile(fst.filePath, os.O_RDWR, fs.FileMode(filePermissions))
	if err != nil {
		log.Panic(err)
	}

	return file
}

// Returning value of key stored on file, or file cursor when valueReader is not nil
func (fs *FileKeyValueStore) Get(key string, valueReader func(cursor.Cursor) ([]byte, error)) ([]byte, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// Validate key
	err := isValidKey(key)
	if err != nil {
		return nil, err
	}

	// Find item position from the index
	itemPosition, exists := fs.keysIndex[key]

	if !exists {
		return nil, nil
	}

	file := fs.openOrPanic()
	defer file.Close()

	return getValueFromPosition(file, itemPosition, nil)
}

// Save key value in file
func (fs *FileKeyValueStore) Set(key string, value []byte) error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// Validate key
	err := isValidKey(key)
	if err != nil {
		return err
	}

	// Validate value
	err = isValidValue(value)
	if err != nil {
		return err
	}

	_, exists := fs.keysIndex[key]

	if exists {

		// TODO: check if its better to deleted every time or to check value and delete only on change
		fs.lock.Unlock()
		currentValue, err := fs.Get(key, nil)
		fs.lock.Lock()

		if err != nil {
			return err
		}

		if equal(value, currentValue) {
			return nil
		} else {
			fs.lock.Unlock()
			fs.Del(key)
			fs.lock.Lock()
		}
	}

	file := fs.openOrPanic()
	defer file.Close()

	itemPosition, err := insertItemToFile(file, key, value)
	if err == nil {
		fs.keysIndex[key] = itemPosition
	}

	return err
}

// Mark key value on file as deleted
func (fs *FileKeyValueStore) Del(key string) error {
	fs.lock.Lock()

	// Validate key
	err := isValidKey(key)
	if err != nil {
		fs.lock.Unlock()
		return err
	}

	// Get item position from index, if not found return error
	itemPosition, exists := fs.keysIndex[key]
	if !exists {
		fs.lock.Unlock()
		return fmt.Errorf("key does not exists")
	}

	delete(fs.keysIndex, key)

	file := fs.openOrPanic()
	defer file.Close()

	err = markItemAsDeletedOnFile(file, itemPosition)
	if err != nil {
		fs.lock.Unlock()
		return err
	}

	// If deleted count is more then <cleanupOnDeletedPercentage> of all the keys, start cleanup
	fs.deletedKeyCount++
	totalKeys := len(fs.keysIndex) + fs.deletedKeyCount
	doCleanup := fs.deletedKeyCount > minDeletedKeyForCleanup && float64(totalKeys)*cleanupOnDeletedRatio <= float64(fs.deletedKeyCount)

	if doCleanup {
		ctx := context.Background()
		go fs.cleanUp(ctx)
	} else {
		fs.lock.Unlock()
	}

	return nil
}

func (fs *FileKeyValueStore) Keys() []string {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	keys := make([]string, len(fs.keysIndex))

	i := 0
	for k := range fs.keysIndex {
		keys[i] = k
		i++
	}

	return keys
}

func (fs *FileKeyValueStore) Flush() {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	fs.keysIndex = make(map[string]int64)
	fs.deletedKeyCount = 0

	os.Remove(fs.filePath)

	// Recrete empty file
	file, _ := os.Create(fs.filePath)
	file.Close()
}

func (fs *FileKeyValueStore) Search(ctx context.Context, evaluate func(value []byte) bool) ([][]byte, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	file := openOrCreate(fs.filePath)
	defer file.Close()

	results := make([][]byte, 0, 1000)

	err := scanFile(ctx, file, true, func(key string, value []byte, deleted bool, filePosition int64) {
		if evaluate(value) {
			results = append(results, value)
		}
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}
