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

	return c
}

func (c *cache) Push(key string, value []byte) {
	if c.maxsize == 0 {
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	kvElement := c.cachedItems[key]

	// If the key already in the cache
	if kvElement != nil {
		kv := keyElementToKeyValue(kvElement)
		kv.value = value
		c.keysList.MoveToFront(kvElement)

		return
	}

	// if we need to remove other key to insert this one just replace the values to avoid allocation
	if len(c.cachedItems)+1 > c.maxsize {
		kvElement := c.keysList.Back()
		kv := keyElementToKeyValue(kvElement)

		delete(c.cachedItems, kv.key)

		kv.key = key
		kv.value = value

		c.cachedItems[key] = kvElement
		c.keysList.MoveToFront(kvElement)

		return
	}

	kvElement = c.keysList.PushFront(&keyValue{key: key, value: value})
	c.cachedItems[key] = kvElement
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

	c.keysList.Remove(kvElement)
	delete(c.cachedItems, key)
}

func (c *cache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Clear cached item
	c.cachedItems = make(map[string]*list.Element)

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
