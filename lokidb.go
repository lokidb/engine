// Fast on-disk key value store
package engine

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/lokidb/engine/consistent"
	"github.com/lokidb/engine/cursor"
	filestore "github.com/lokidb/engine/file_storage"
	lrucache "github.com/lokidb/engine/lrucache"
)

const fileExtension = ".loki"
const filePrefix = "ldb-"
const aolFilename = "mutations_log"

// Features flags
const toggleAOL = false

type storage struct {
	rootPath   string
	aolPath    string
	aolLock    sync.Mutex
	lruCache   lrucache.Cache
	fileStores map[string]*filestore.FileKeyValueStore
	filesRing  consistent.ConsistentHash
}

type KeyValueStore interface {
	Set(string, []byte) error
	Get(string, func(cursor.Cursor) ([]byte, error)) []byte
	Del(string) bool
	Keys() []string
	Flush()
	Search(func(value []byte) bool) ([][]byte, error)
}

func New(rootPath string, cacheSize int, filesCount int) KeyValueStore {
	s := new(storage)

	s.rootPath = rootPath
	s.aolPath = filepath.Join(rootPath, aolFilename+fileExtension)
	s.lruCache = lrucache.New(cacheSize)
	s.fileStores = createFileStores(s.rootPath, filesCount)
	s.filesRing, _ = consistent.New(1024)

	for filename := range s.fileStores {
		(s.filesRing).AddMember(filename)
	}

	return s
}

func (s *storage) Set(key string, value []byte) error {
	s.appendToLog("SET", key, value)

	filename := s.filesRing.GetMemberForKey(key)
	fileStore := s.fileStores[filename]

	s.lruCache.Push(key, value)
	err := fileStore.Set(key, value)
	return err
}

// get key from storage, specify valueReader to read only specific section from the value
// or nil for the full value
func (s *storage) Get(key string, valueReader func(cursor.Cursor) ([]byte, error)) []byte {
	value := s.lruCache.Get(key)
	if value != nil {
		return value
	}

	filename := s.filesRing.GetMemberForKey(key)
	fileStore := s.fileStores[filename]

	value, _ = fileStore.Get(key, valueReader)
	s.lruCache.Push(key, value)

	return value
}

func (s *storage) Del(key string) bool {
	s.appendToLog("DEL", key, nil)

	filename := s.filesRing.GetMemberForKey(key)
	fileStore := s.fileStores[filename]

	s.lruCache.Del(key)
	err := fileStore.Del(key)

	return err == nil
}

func (s *storage) Keys() []string {
	keys := make([]string, 0, 10000)

	for _, filestore := range s.fileStores {
		keys = append(keys, filestore.Keys()...)
	}

	return keys
}

// Delete all files and clear all RAM data
func (s *storage) Flush() {
	if toggleAOL {
		s.aolLock.Lock()
		defer s.aolLock.Unlock()
		os.Remove(s.aolPath)
	}

	s.lruCache.Clear()

	var wg sync.WaitGroup

	for _, fs := range s.fileStores {
		wg.Add(1)

		go func(fs *filestore.FileKeyValueStore) {
			fs.Flush()
			wg.Done()
		}(fs)
	}

	wg.Wait()
}

// scan all the values in the store and filter them with the 'evaluate' function
func (s *storage) Search(evaluate func(value []byte) bool) ([][]byte, error) {
	results := make([][]byte, 0, 1000)
	for _, fs := range s.fileStores {
		fsResults, err := fs.Search(evaluate)
		if err != nil {
			return nil, err
		}

		results = append(results, fsResults...)
	}

	return results, nil
}
