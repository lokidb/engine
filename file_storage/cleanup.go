package filestore

import (
	"io/fs"
	"os"
)

func (fst *FileKeyValueStore) cleanUp() error {
	fst.lock.Lock()
	defer fst.lock.Unlock()

	file, err := os.OpenFile(fst.filePath, os.O_RDWR, fs.FileMode(filePermissions))
	if err != nil {
		panic(err)
	}

	cleanFile, err := os.Create(fst.filePath + cleanFileExtension)
	if err != nil {
		return err
	}

	err = scanFile(file, true, func(key string, value []byte, deleted bool, filePosition int64) {
		if !deleted {
			itemPosition, err := insertItemToFile(cleanFile, key, value)
			if err != nil {
				panic(err)
			}
			fst.keysIndex[key] = itemPosition
		}
	})

	if err != nil {
		return err
	}

	cleanFile.Close()
	file.Close()

	fst.deletedKeyCount = 0

	if err = os.Remove(fst.filePath); err != nil {
		return err
	}

	if err = os.Rename(fst.filePath+cleanFileExtension, fst.filePath); err != nil {
		panic(err)
	}

	return nil
}
