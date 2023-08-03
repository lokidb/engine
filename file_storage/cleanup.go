package filestore

import (
	"context"
	"io/fs"
	"os"
)

// Check deleted keys ratio and decide if cleanup is required
func (fst *FileKeyValueStore) isCleanupRequired() bool {
	// If deleted count is more then <cleanupOnDeletedPercentage> of all the keys, start cleanup
	fst.deletedKeyCount++
	totalKeys := len(fst.keysIndex) + fst.deletedKeyCount
	doCleanup := fst.deletedKeyCount > minDeletedKeyForCleanup && float64(totalKeys)*cleanupOnDeletedRatio <= float64(fst.deletedKeyCount)

	return doCleanup
}

func (fst *FileKeyValueStore) cleanUp(ctx context.Context) error {
	fst.lock.Lock()
	defer fst.lock.Unlock()

	// Open items file
	file, err := os.OpenFile(fst.filePath, os.O_RDWR, fs.FileMode(filePermissions))
	if err != nil {
		panic(err)
	}

	// Create new file for non-deleted items
	cleanFile, err := os.Create(fst.filePath + cleanFileExtension)
	if err != nil {
		return err
	}

	// Scan currenct file and insert all non-deleted items to new file
	err = scanFile(ctx, file, true, func(key string, value []byte, deleted bool, filePosition int64) {
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

	fst.deletedKeyCount = 0

	// Close both files delete the old one and rename the updated one
	cleanFile.Close()
	file.Close()

	if err = os.Remove(fst.filePath); err != nil {
		return err
	}

	if err = os.Rename(fst.filePath+cleanFileExtension, fst.filePath); err != nil {
		panic(err)
	}

	return nil
}
