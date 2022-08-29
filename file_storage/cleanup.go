package filestore

import "os"

func (fs *FileKeyValueStore) cleanUp() error {
	defer fs.cleanupLock.Done()

	file := fs.openOrPanic()

	cleanFile, err := os.Create(fs.filePath + cleanFileExtension)
	if err != nil {
		return err
	}

	err = scanFile(file, true, func(key string, value []byte, deleted bool, filePosition int64) {
		if !deleted {
			itemPosition, err := insertItemToFile(cleanFile, key, value)
			if err != nil {
				panic(err)
			}

			fs.keysIndex[key] = itemPosition
		}
	})

	if err != nil {
		return err
	}

	cleanFile.Close()
	fs.close(file)

	fs.deletedKeyCount = 0

	if err = os.Remove(fs.filePath); err != nil {
		return err
	}

	if err = os.Rename(fs.filePath+cleanFileExtension, fs.filePath); err != nil {
		panic(err)
	}

	return nil
}
