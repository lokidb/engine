package filestore

import (
	"os"
	"strconv"
	"sync"
	"testing"
)

func TestFullUse(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("./testfile")
	})

	db := New("./testfile")

	db.Set("a", []byte{97})

	value, err := db.Get("a", nil)
	if err != nil {
		t.Fatal(err)
	}

	if value[0] != 97 {
		t.Errorf("expectnig value 97 not %d", value)
	}

	if err = db.Del("a"); err != nil {
		t.Fatal(err)
	}

	value, err = db.Get("a", nil)
	if err != nil {
		t.Fatal(err)
	}

	if value != nil {
		t.Errorf("expected nil return for deleted key")
	}
}

func TestCleanup(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("./testfile5.test")
	})

	db := New("./testfile5.test")

	for i := 0; i < 1000; i++ {
		err := db.Set(strconv.Itoa(i), []byte{45, 84})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 1000; i++ {
		err := db.Del(strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	fileInfo, err := os.Stat("./testfile5.test")
	if err != nil {
		t.Fatal(err)
	}

	if fileInfo.Size() > 500*7 { // minNotCleanedKeys(50) * (headerSize(5) + value(2))
		t.Errorf("expected the file to be fully cleaned")
	}
}

func TestKeys(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("./testfile8.test")
	})

	db := New("./testfile8.test")

	for i := 0; i < 1000; i++ {
		err := db.Set(strconv.Itoa(i), []byte{45, 84})
		if err != nil {
			t.Fatal(err)
		}
	}

	keys := db.Keys()

	if len(keys) != 1000 {
		t.Error("keys lenght expected to be 1000")
	}
}

func TestFlush(t *testing.T) {
	db := New("./testfile.test")

	for i := 0; i < 1000; i++ {
		err := db.Set(strconv.Itoa(i), []byte{45, 84})
		if err != nil {
			t.Fatal(err)
		}
	}

	db.Del("5")

	db.Flush()

	if len(db.keysIndex) > 0 {
		t.Error("expecting length of keys index to be 0 after flush")
	}

	if db.deletedKeyCount > 0 {
		t.Error("expecting deleted keys count to be 0 after flush")
	}
}

func TestSearch(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("./testfile.test")
	})

	db := New("./testfile.test")

	for i := 0; i < 231; i++ {
		err := db.Set(strconv.Itoa(i), []byte{byte(i), 84})
		if err != nil {
			t.Fatal(err)
		}
	}

	searchResults, err := db.Search(func(value []byte) bool {
		return int(value[0]) >= 230
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(searchResults) != 1 {
		t.Error("expecting search results to include one result")
	}

	if int(searchResults[0][0]) != 230 {
		t.Error("expecting result first byte to be 999")
	}
}

func TestFlushKeepFile(t *testing.T) {
	db := New("./testfile2.test")

	db.Set("abc", []byte("asdassdasd"))

	db.Flush()

	err := os.Remove("./testfile2.test")
	if err != nil {
		t.Error("expecting the file to exist after flush")
	}
}

func TestThreadSafe(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("./testfile3.test")
	})

	db := New("./testfile3.test")

	var wg sync.WaitGroup

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			db.Set("a", []byte("b"))
			wg.Done()
		}()
	}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			db.Get("a", nil)
			wg.Done()
		}()
	}

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			db.Del("a")
			wg.Done()
		}()
	}

	wg.Wait()
}
