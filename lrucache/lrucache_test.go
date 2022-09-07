package lrucache

import "testing"

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

func TestMoveToTop(t *testing.T) {
	c := New(50)

	c.Push("a", []byte("a"))
	c.Push("b", []byte("b"))
	c.Push("c", []byte("c"))

	if !equal(c.Get("a"), []byte("a")) {
		t.Errorf("expecting key 'a' to return 'a'")
	}

	c.Push("d", []byte("d"))
	c.Push("e", []byte("e"))

	if !equal(c.Get("a"), []byte("a")) {
		t.Errorf("expecting key 'a' to still return 'a'")
	}
}

func TestDel(t *testing.T) {
	c := New(50)

	c.Push("a", []byte("a"))
	c.Push("b", []byte("b"))
	c.Push("c", []byte("c"))

	c.Del("b")

	if c.Get("b") != nil {
		t.Errorf("expecting key 'b' to return nil")
	}
}

func TestClear(t *testing.T) {
	c := New(50)

	c.Push("a", []byte("a"))
	c.Push("b", []byte("b"))
	c.Push("c", []byte("c"))

	c.Clear()

	if c.Get("a") != nil {
		t.Errorf("expecting key 'a' to return nil")
	}

	if c.Get("b") != nil {
		t.Errorf("expecting key 'b' to return nil")
	}

	if c.Get("c") != nil {
		t.Errorf("expecting key 'c' to return nil")
	}
}
