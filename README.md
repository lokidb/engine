# lokidb engine
LokiDB storage engine

[![Test](https://github.com/lokidb/engine/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/lokidb/engine/actions/workflows/test.yml)
---

#### Import
```shell
go get github.com/hvuhsg/lokidb/engine
```

#### Features
- storage on disk for consistency
- in-memory LRU cache for read optimization
- deleted keys cleanup for disk space saving
- distribution of keys across multiple files for maximazing file access time

#### Interface
```go
type KeyValueStore interface {
    Set(string, []byte) error
    Get(string, func(cursor.Cursor) ([]byte, error)) []byte
    Del(string) bool
    Keys() []string
    Flush()
    Search(func(value []byte) bool) ([][]byte, error)
}
```


#### Example
```go
package main

import (
	"fmt"

	"github.com/hvuhsg/lokidb/engine"
)

func main() {
	filesDir := "./"
	cacheSize := 20000
	numberOfFiles := 5

	db := engine.New(filesDir, cacheSize, numberOfFiles)

	db.Set("name", []byte("mosh"))
	db.Set("age", []byte{5})

	name := db.Get("name")     // []byte("mosh")
	fmt.Printf("%s\n", name)

	deleted := db.Del("name")  // true
	
	name = db.Get("name", nil) // nil
	fmt.Println(name, deleted)
}
```