package engine

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	filestore "github.com/lokidb/engine/file_storage"
)

func createFileStores(rootPath string, filesCount int) map[string]*filestore.FileKeyValueStore {
	fileStores := make(map[string]*filestore.FileKeyValueStore, filesCount)

	for i := 0; i < filesCount; i++ {
		filename := filePrefix + strconv.Itoa(i) + fileExtension
		filePath := filepath.Join(rootPath, filename)
		fileStore := filestore.New(filePath)
		fileStores[filename] = fileStore
	}

	return fileStores
}

func (s *storage) appendToLog(command string, key string, value []byte) {
	if !toggleAOL {
		return
	}

	s.aolLock.Lock()
	defer s.aolLock.Unlock()

	file, err := os.OpenFile(s.aolPath, os.O_WRONLY, os.ModeAppend)
	if err != nil {
		file, err = os.Create(s.aolPath)
		if err != nil {
			panic(err)
		}
	}

	endOffset, _ := file.Seek(0, io.SeekEnd)

	file.WriteAt([]byte(fmt.Sprintf("%s -:- %s -:- %v\n", command, key, value)), endOffset)
}

func equals(a []byte, b []byte) bool {
	if a == nil && b == nil {
		return true
	} else if a == nil || b == nil {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
