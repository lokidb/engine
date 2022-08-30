// Package filestore provide thraed-safe interface for storing key value pairs on a single file
// the package includes automatic deleted keys cleanup when the number of deleted
// keys become more then 30% of the total count.
package filestore

import (
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
const minDeletedKeyForCleanup = 50
const cleanFileExtension = ".clean"

type FileKeyValueStore struct {
	filePath        string
	keysIndex       map[string]int64
	deletedKeyCount int
	fileLock        sync.Mutex
	indexLock       sync.RWMutex
	cleanupLock     sync.WaitGroup
}

func New(filePath string) *FileKeyValueStore {
	fs := new(FileKeyValueStore)
	fs.filePath = filePath

	keysIndex, deletedKeysCount, err := createKeysIndex(filePath)
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
		log.Panic(err)
	}

	return file
}

func (fst *FileKeyValueStore) openOrPanic() *os.File {
	fst.fileLock.Lock()

	file, err := os.OpenFile(fst.filePath, os.O_RDWR, fs.FileMode(filePermissions))
	if err != nil {
		fst.fileLock.Unlock()
		log.Panic(err)
	}

	return file
}

func (fs *FileKeyValueStore) close(file *os.File) {
	defer fs.fileLock.Unlock()
	file.Close()
}

// Returning value of key stored on file, or file cursor when valueReader is not nil
func (fs *FileKeyValueStore) Get(key string, valueReader func(cursor.Cursor) ([]byte, error)) ([]byte, error) {
	// Validate key
	err := isValidKey(key)
	if err != nil {
		return nil, err
	}

	// Wait for the cleanup to end when it accure
	fs.cleanupLock.Wait()

	// Find item position from the index
	itemPosition, exists := fs.safeGet(key)

	if !exists {
		return nil, nil
	}

	file := fs.openOrPanic()
	defer fs.close(file)

	return getValueFromPosition(file, itemPosition, nil)
}

// Save key value in file
func (fs *FileKeyValueStore) Set(key string, value []byte) error {
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

	// Wait for the cleanup to end when it accure
	fs.cleanupLock.Wait()

	_, exists := fs.safeGet(key)

	if exists {
		// TODO: check if its better to deleted every time or to check value and delete only on change
		currentValue, err := fs.Get(key, nil)
		if err != nil {
			return err
		}

		if equal(value, currentValue) {
			return nil
		} else {
			fs.Del(key)
		}
	}

	file := fs.openOrPanic()
	defer fs.close(file)

	itemPosition, err := insertItemToFile(file, key, value)
	if err == nil {
		fs.safeSet(key, itemPosition)
	}

	return err
}

// Mark key value on file as deleted
func (fs *FileKeyValueStore) Del(key string) error {
	// Validate key
	err := isValidKey(key)
	if err != nil {
		return err
	}

	// Wait for the cleanup to end when it accure
	fs.cleanupLock.Wait()

	// Get item position from index, if not found return error
	itemPosition, exists := fs.safeGet(key)
	if !exists {
		return fmt.Errorf("key does not exists")
	}

	fs.safeDel(key)

	file := fs.openOrPanic()
	defer fs.close(file)

	err = markItemAsDeletedOnFile(file, itemPosition)
	if err != nil {
		return err
	}

	// If deleted count is more then <cleanupOnDeletedPercentage> of all the keys, start cleanup
	fs.deletedKeyCount++
	totalKeys := fs.safeLen() + fs.deletedKeyCount
	if fs.deletedKeyCount > minDeletedKeyForCleanup && float64(totalKeys)*cleanupOnDeletedRatio <= float64(fs.deletedKeyCount) {
		fs.cleanupLock.Add(1)
		go fs.cleanUp()
	}

	return nil
}

func (fs *FileKeyValueStore) Keys() []string {
	fs.indexLock.RLock()
	defer fs.indexLock.RUnlock()

	keys := make([]string, fs.safeLen())

	i := 0
	for k := range fs.keysIndex {
		keys[i] = k
		i++
	}

	return keys
}

func (fs *FileKeyValueStore) Flush() {
	fs.fileLock.Lock()
	defer fs.fileLock.Unlock()
	fs.indexLock.Lock()
	defer fs.indexLock.Unlock()
	fs.cleanupLock.Wait()

	fs.keysIndex = make(map[string]int64)
	fs.deletedKeyCount = 0

	os.Remove(fs.filePath)

	// Recrete empty file
	file, _ := os.Create(fs.filePath)
	file.Close()
}

func (fs *FileKeyValueStore) Search(evaluate func(value []byte) bool) ([][]byte, error) {
	file := openOrCreate(fs.filePath)
	defer file.Close()

	results := make([][]byte, 0, 1000)

	err := scanFile(file, true, func(key string, value []byte, deleted bool, filePosition int64) {
		if evaluate(value) {
			results = append(results, value)
		}
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}
