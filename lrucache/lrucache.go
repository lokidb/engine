// Simple LRU cache for storing key-value data
package lrucache

import (
	"container/list"
	"fmt"
	"sync"
)

type keyValue struct {
	key   string
	value []byte
}

type cache struct {
	keysList    *list.List
	maxsize     int
	currentSize int
	cachedItems map[string]*list.Element
	lock        sync.Mutex
}

type Cache interface {
	Push(key string, value []byte)
	Get(key string) []byte
	Del(key string)
	Clear()
}

func New(maxsize int) Cache {
	c := new(cache)
	c.keysList = list.New()
	c.cachedItems = make(map[string]*list.Element, maxsize+1)
	c.maxsize = maxsize
	c.currentSize = 0

	return c
}

func (c *cache) Push(key string, value []byte) {
	if c.maxsize == 0 {
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	kvElement := c.cachedItems[key]
	kvSize := len(value) + len(key)

	// If the key already in the cache
	if kvElement != nil {
		kv := keyElementToKeyValue(kvElement)
		kv.value = value
		c.keysList.MoveToFront(kvElement)

		return
	}

	// remove items until there is enough space for the new item
	for kvSize < c.maxsize && len(c.cachedItems) > 0 && c.currentSize+kvSize > c.maxsize {
		kvElement := c.keysList.Back()
		kv := keyElementToKeyValue(kvElement)

		delete(c.cachedItems, kv.key)
		c.keysList.Remove(kvElement)
	}

	kvElement = c.keysList.PushFront(&keyValue{key: key, value: value})
	c.cachedItems[key] = kvElement
	c.currentSize += kvSize
}

func (c *cache) Get(key string) []byte {
	c.lock.Lock()
	defer c.lock.Unlock()

	kvElement := c.cachedItems[key]
	if kvElement == nil {
		return nil
	}

	kv := keyElementToKeyValue(kvElement)

	c.keysList.MoveToFront(kvElement)

	return kv.value
}

func (c *cache) Del(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	kvElement := c.cachedItems[key]
	if kvElement == nil {
		return
	}

	kvSize := len(keyElementToKeyValue(kvElement).value) + len(key)

	c.keysList.Remove(kvElement)
	delete(c.cachedItems, key)
	c.currentSize -= kvSize
}

func (c *cache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Clear cached item
	c.cachedItems = make(map[string]*list.Element)
	c.currentSize = 0

	// Clear list
	c.keysList.Init()
}

func keyElementToKeyValue(ke *list.Element) *keyValue {
	kv, ok := ke.Value.(*keyValue)
	if !ok {
		panic(fmt.Errorf("kv always must be of type *keyValue"))
	}

	return kv
}
