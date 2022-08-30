package filestore

func (fs *FileKeyValueStore) safeGet(key string) (int64, bool) {
	fs.indexLock.RLock()
	defer fs.indexLock.RUnlock()

	v, ok := fs.keysIndex[key]
	return v, ok
}

func (fs *FileKeyValueStore) safeSet(key string, position int64) {
	fs.indexLock.Lock()
	defer fs.indexLock.Unlock()

	fs.keysIndex[key] = position
}

func (fs *FileKeyValueStore) safeDel(key string) {
	fs.indexLock.Lock()
	defer fs.indexLock.Unlock()

	delete(fs.keysIndex, key)
}
