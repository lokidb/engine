package engine

import (
	"os"
	"strconv"
	"testing"
)

func equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}

	return true
}

func TestValidation(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("ldb-0.loki")
	})

	engine := New("./", 0, 1)

	err := engine.Set("", []byte("abc"))
	if err == nil {
		t.Errorf("expecting validation check to return an error")
	}

	err = engine.Set("abc", []byte(""))
	if err == nil {
		t.Errorf("expecting validation check to return an error")
	}

	val := engine.Get("", nil)
	if val != nil {
		t.Errorf("expecting to get nil for invalid key")
	}
}

func TestGetNonExisting(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("ldb-0.loki")
	})

	engine := New("./", 0, 1)

	err := engine.Get("a", nil)
	if err != nil {
		t.Errorf("expecting to get nil for non existing key")
	}
}

func TestEngine(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("ldb-0.loki")
	})

	engine := New("./", 0, 1)

	ValidKeyValue := map[string][]byte{
		"a":   []byte("b"),
		"123": []byte("321"),
	}

	for key, value := range ValidKeyValue {
		t.Run("valid key value", func(t *testing.T) {
			err := engine.Set(key, value)
			if err != nil {
				t.Fatalf("setting value %s for key %s should be valid", value, key)
			}

			retValue := engine.Get(key, nil)
			if !equal(retValue, value) {
				t.Fatalf("return value from engine for key %s must be %s and not %s", key, value, retValue)
			}

			deleted := engine.Del(key)
			if !deleted {
				t.Fatalf("key %s is exists and shoule be deleted", key)
			}

			retValue = engine.Get(key, nil)
			if retValue != nil {
				t.Fatalf("return value from engine for deleted key %s must be nil not %s", key, retValue)
			}
		})
	}

	InvalidKeyValue := map[string][]byte{
		"a": []byte(""),
		"":  []byte("123"),
	}

	for key, value := range InvalidKeyValue {
		t.Run("invalid key value", func(t *testing.T) {
			err := engine.Set(key, value)
			if err == nil {
				t.Fatalf("setting value %s for key %s should be invalid", value, key)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("ldb-0.loki")
	})

	engine := New("./", 5, 1)

	err := engine.Set("key", []byte("value"))
	if err != nil {
		t.Error(err)
	}

	value := engine.Get("key", nil)
	if !equal(value, []byte("value")) {
		t.Errorf("expecting value to be equal to 'value'")
	}

	deleted := engine.Del("key")
	if !deleted {
		t.Errorf("expecting key to be deleted")
	}

	value = engine.Get("key", nil)
	if value != nil {
		t.Errorf("expecting value to be equal to nil after key deleted")
	}
}

func TestUseDiskData(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("ldb-0.loki")
	})

	db := New("./", 1000, 1)
	db.Set("abc", []byte("abc"))
	db.Set("abc4", []byte("abc4"))
	db.Del("abc4")

	db2 := New("./", 1000, 1)
	if !equal(db2.Get("abc", nil), []byte("abc")) {
		t.Errorf("cant use loaded data")
	}
	if equal(db2.Get("abc4", nil), []byte("abc4")) {
		t.Errorf("loaded data is loading deleted keys")
	}
}

func TestKeys(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("ldb-0.loki")
		os.Remove("ldb-1.loki")
	})

	db := New("./", 0, 2)

	for i := 0; i < 1000; i++ {
		db.Set(strconv.Itoa(i), []byte("valuevalue"))
	}

	keys := db.Keys()
	if len(keys) != 1000 {
		t.Error("expecting length of keys to be 1000")
	}
}

func TestSearch(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("ldb-0.loki")
		os.Remove("ldb-1.loki")
		os.Remove("ldb-2.loki")
		os.Remove("ldb-3.loki")
		os.Remove("ldb-4.loki")
	})

	db := New("./", 100, 5)

	for i := 0; i < 231; i++ {
		db.Set(strconv.Itoa(i), []byte{byte(i)})
	}

	searchResults, err := db.Search(func(value []byte) bool {
		return value[0] >= 230
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(searchResults) != 1 {
		t.Error("expecting search results to contain only on result")
	}

	if searchResults[0][0] != 230 {
		t.Error("expecting search result to be 230")
	}
}

func TestOverwrite(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("ldb-0.loki")
		os.Remove("ldb-1.loki")
		os.Remove("ldb-2.loki")
		os.Remove("ldb-3.loki")
		os.Remove("ldb-4.loki")
	})

	db := New("./", 100, 5)

	db.Set("abc", []byte("b0123456789"))

	db.Get("abc", nil)

	db.Set("abc", []byte("0123456789"))

	value := db.Get("abc", nil)
	if !equal(value, []byte("0123456789")) {
		t.Error("expecting key 'abc' to be overwriten")
	}
}
